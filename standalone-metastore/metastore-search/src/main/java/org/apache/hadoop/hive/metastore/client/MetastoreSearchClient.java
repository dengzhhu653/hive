/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore.client;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.SearchTable;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SearchTablesRequest;
import org.apache.hadoop.hive.metastore.api.TableSearchResponse;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hive.search.config.SearchOptions;
import org.apache.hive.search.exception.IndexNotHealthyException;
import org.apache.hive.search.exception.IndexNotReadyException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Failover-aware search client for HA metastore deployments. */
public final class MetastoreSearchClient implements SearchTable, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(MetastoreSearchClient.class);

  private final Configuration configuration;
  private final URI[] uris;
  private final MetastoreBlacklist blacklist;
  private final int retryLimit;
  private final long retryDelaySeconds;
  private final ConnectionCache connectionCache;

  public MetastoreSearchClient(Configuration configuration) {
    this(configuration, MetastoreBlacklist.shared());
  }

  MetastoreSearchClient(Configuration configuration, MetastoreBlacklist blacklist) {
    this.configuration = configuration;
    this.uris = parseMetastoreUris(configuration);
    this.blacklist = blacklist;
    SearchOptions options = new SearchOptions(configuration);
    blacklist.setBlockTtlMs(
        TimeUnit.SECONDS.toMillis(options.getClientBlacklistTtlSeconds()));
    this.retryLimit = MetastoreConf.getIntVar(configuration, ConfVars.THRIFT_FAILURE_RETRIES);
    this.retryDelaySeconds = MetastoreConf.getTimeVar(
        configuration, ConfVars.CLIENT_CONNECT_RETRY_DELAY, TimeUnit.SECONDS);
    this.connectionCache = new ConnectionCache();
  }

  /** Search tables across configured metastore URIs with HA failover. */
  @Override
  public TableSearchResponse searchTables(SearchTablesRequest request)
      throws TException, IOException {
    SecurityUtils.reloginExpiringKeytabUser();
    return searchTablesWithFailover(request);
  }

  @Override
  public void close() {
    connectionCache.close();
  }

  private TableSearchResponse searchTablesWithFailover(SearchTablesRequest request)
      throws TException, IOException {
    FailoverState state = new FailoverState();
    List<String> uriKeysToTry = connectionCache.chooseUris(uris, blacklist);

    for (String uriKey : uriKeysToTry) {
      state.candidates++;
      try {
        ThriftSearchTableClient client = connectionCache.getOrOpen(uriKey);
        return searchWithTransportRetry(client, uriKey, request);
      } catch (org.apache.hadoop.hive.metastore.api.IndexNotHealthyException e) {
        handleUnhealthy(uriKey, e, state);
      } catch (org.apache.hadoop.hive.metastore.api.IndexNotReadyException e) {
        state.updateBestIndexNotReady(e.getMessage());
      } catch (TTransportException e) {
        LOG.warn("Metastore {} unavailable after transport retries: {}", uriKey, e.getMessage());
        connectionCache.invalidate(uriKey);
        state.lastTransportError = e;
      }
    }

    return state.toExceptionOrThrow(uris.length);
  }

  private void handleUnhealthy(String uriKey,
      org.apache.hadoop.hive.metastore.api.IndexNotHealthyException e,
      FailoverState state) {
    LOG.warn("Metastore {} reported unhealthy search index: {}", uriKey, e.getMessage());
    connectionCache.invalidate(uriKey);
    blacklist.block(uriKey, e.getMessage());
    state.unhealthyErrors.add(new IndexNotHealthyException(e.getMessage()));
  }

  private TableSearchResponse searchWithTransportRetry(
      ThriftSearchTableClient client,
      String uriKey,
      SearchTablesRequest request) throws TException {
    int retriesMade = 0;
    while (true) {
      try {
        SecurityUtils.reloginExpiringKeytabUser();
        if (retriesMade > 0) {
          client.reconnect();
        }
        return client.searchTables(request);
      } catch (TTransportException e) {
        if (retriesMade >= retryLimit || client.isLocalMetaStore()) {
          throw e;
        }
        retriesMade++;
        LOG.warn(
            "Search client lost connection to {}. Attempting to reconnect ({} of {}) after {}s.",
            uriKey, retriesMade, retryLimit, retryDelaySeconds, e);
        sleepBeforeRetry();
      }
    }
  }

  private void sleepBeforeRetry() throws MetaException {
    try {
      Thread.sleep(retryDelaySeconds * 1000);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new MetaException("Interrupted while retrying metastore search: " + ie.getMessage());
    }
  }

  static URI[] parseMetastoreUris(Configuration configuration) {
    String thriftUris = MetastoreConf.getVar(configuration, MetastoreConf.ConfVars.THRIFT_URIS);
    if (StringUtils.isBlank(thriftUris)) {
      return new URI[0];
    }
    List<URI> parsed = new ArrayList<>();
    for (String uriText : thriftUris.split(",")) {
      String trimmed = uriText.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      if (!trimmed.contains("://")) {
        trimmed = "thrift://" + trimmed;
      }
      parsed.add(URI.create(trimmed));
    }
    Collections.shuffle(parsed);
    return parsed.toArray(new URI[0]);
  }

  /** Cached Thrift connection with URI selection and blacklist awareness. */
  private final class ConnectionCache {
    private final Object connectionLock = new Object();
    private CachedConnection activeConnection;

    /**
     * Returns URI keys to try: active cached URI first when valid and not blacklisted,
     * then remaining configured URIs that are not blacklisted.
     */
    List<String> chooseUris(URI[] uris, MetastoreBlacklist blacklist) {
      List<String> result = new ArrayList<>();
      String cachedUriKey = null;

      synchronized (connectionLock) {
        if (activeConnection != null
            && activeConnection.client.isActive()
            && !blacklist.isBlocked(activeConnection.uriKey)) {
          cachedUriKey = activeConnection.uriKey;
          result.add(cachedUriKey);
        } else if (activeConnection != null) {
          closeLocked();
        }
      }

      for (URI uri : uris) {
        String uriKey = uri.toString();
        if (uriKey.equals(cachedUriKey)) {
          continue;
        }
        if (blacklist.isBlocked(uriKey)) {
          continue;
        }
        result.add(uriKey);
      }

      return result;
    }

    ThriftSearchTableClient getOrOpen(String uriKey) throws MetaException {
      synchronized (connectionLock) {
        if (activeConnection != null
            && activeConnection.uriKey.equals(uriKey)
            && !blacklist.isBlocked(uriKey)
            && activeConnection.client.isActive()) {
          LOG.debug("Reusing active metastore search connection to {}", uriKey);
          return activeConnection.client;
        }
        closeLocked();
        Configuration conf = new Configuration(configuration);
        MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, uriKey);
        ThriftSearchTableClient client = new ThriftSearchTableClient(conf);
        activeConnection = new CachedConnection(uriKey, client);
        return client;
      }
    }

    void invalidate() {
      synchronized (connectionLock) {
        closeLocked();
      }
    }

    void invalidate(String uriKey) {
      synchronized (connectionLock) {
        if (activeConnection != null && activeConnection.uriKey.equals(uriKey)) {
          closeLocked();
        }
      }
    }

    void close() {
      invalidate();
    }

    private void closeLocked() {
      if (activeConnection != null) {
        activeConnection.client.close();
        activeConnection = null;
      }
    }

    private final class CachedConnection {
      private final String uriKey;
      private final ThriftSearchTableClient client;

      private CachedConnection(String uriKey, ThriftSearchTableClient client) {
        this.uriKey = uriKey;
        this.client = client;
      }
    }
  }

  private static final class FailoverState {
    private final List<IndexNotHealthyException> unhealthyErrors = new ArrayList<>();
    private String bestIndexNotReady;
    private TTransportException lastTransportError;
    private int candidates;

    private void updateBestIndexNotReady(String message) {
      if (message != null
          && (bestIndexNotReady == null || message.length() > bestIndexNotReady.length())) {
        bestIndexNotReady = message;
      }
    }

    private TableSearchResponse toExceptionOrThrow(int uriCount) throws TException, IOException {
      if (bestIndexNotReady != null) {
        throw new IndexNotReadyException(bestIndexNotReady);
      }
      if (!unhealthyErrors.isEmpty()) {
        throw unhealthyErrors.get(0);
      }
      if (lastTransportError != null) {
        throw lastTransportError;
      }
      if (candidates == 0 && uriCount > 0) {
        throw new IndexNotHealthyException("All metastore instances are blacklisted for search");
      }
      throw new MetaException("No metastore URIs configured or available for search");
    }
  }
}
