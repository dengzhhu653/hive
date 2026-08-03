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

package org.apache.hadoop.hive.metastore.handler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.MetaStoreFilterHook;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SearchMode;
import org.apache.hadoop.hive.metastore.api.SearchTablesRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableSearchResponse;
import org.apache.hadoop.hive.metastore.utils.FilterUtils;
import org.apache.hive.search.config.SearchOptions;
import org.apache.hive.search.exception.InitializeException;
import org.apache.hive.search.search.SearchQuery;
import org.apache.hive.search.search.TableSearchHit;
import org.apache.hive.search.search.TableSearchResult;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.ExceptionHandler.handleException;

@RequestHandler(requestBody = SearchTablesRequest.class)
public class SearchTablesHandler
    extends AbstractRequestHandler<SearchTablesRequest, SearchTablesHandler.SearchTablesResult> {
  private static final Logger LOG = LoggerFactory.getLogger(SearchTablesHandler.class);

  private final Configuration configuration;
  private MetaStoreFilterHook filterHook;

  SearchTablesHandler(IHMSHandler handler, SearchTablesRequest request) {
    super(handler, false, request);
    this.configuration = handler.getConf();
  }

  @Override
  protected void beforeExecute() throws TException, IOException {
    this.filterHook = handler.getMetaFilterHook();
    if (!new SearchOptions(configuration).isEnabled()) {
      throw new TException("Search is disabled, turn on metastore.search.enabled to support it");
    }
    if (!HiveMetaStore.isMetaStoreRemote()) {
      throw new TException("Only remote Metastore can statisfy this request");
    }
    try {
      SearchProvider.install(configuration);
      LOG.info("Installed Metastore table search provider");
    } catch (InitializeException e) {
      String message = "Failed to install Metastore table search provider";
      LOG.error(message, e);
      throw new TException(message + ": " + e.getMessage());
    }
  }

  @Override
  protected SearchTablesResult execute() throws TException {
    try {
      SearchQuery query = SearchQuery.fromQueryBody(
          request.getQueryBody(),
          toQueryMode(request.getMode()),
          request.getCatalogName(),
          request.getDatabaseName(),
          request.isSetLimit() ? request.getLimit() : 0);
      TableSearchResult result = filterAccessibleHits(SearchProvider.get().search(query));
      return new SearchTablesResult(result, true);
    } catch (Exception e) {
      throw handleException(e)
          .convertIfInstance(
              org.apache.hive.search.exception.IndexNotReadyException.class,
              org.apache.hadoop.hive.metastore.api.IndexNotReadyException.class)
          .convertIfInstance(
              org.apache.hive.search.exception.IndexNotHealthyException.class,
              org.apache.hadoop.hive.metastore.api.IndexNotHealthyException.class)
          .defaultTException();
    }
  }

  private TableSearchResult filterAccessibleHits(TableSearchResult result) throws MetaException {
    if (filterHook == null || result.hits().isEmpty()) {
      return result;
    }

    Map<TableName, TableSearchHit> tableToHits = new HashMap<>();
    List<Table> tables = new ArrayList<>(result.hits().size());
    for (TableSearchHit hit : result.hits()) {
      tables.add(tableForFilter(hit));
      tableToHits.put(hit.name(), hit);
    }
    List<Table> accessibleTables = FilterUtils.filterTablesIfEnabled(true, filterHook, tables);

    List<TableSearchHit> filteredHits = new ArrayList<>(accessibleTables.size());
    for (Table table : accessibleTables) {
      TableSearchHit hit =
          tableToHits.get(new TableName(table.getCatName(), table.getDbName(), table.getTableName()));
      if (hit != null) {
        filteredHits.add(hit);
      }
    }
    return new TableSearchResult(filteredHits, result.total(), result.processedEventId());
  }

  private Table tableForFilter(TableSearchHit hit) {
    Table table = hit.table();
    if (table != null) {
      return table;
    }
    TableName name = hit.name();
    Table stub = new Table(name.getTable(), name.getDb(), null, 0, 0, 0, null, null, null, null, null, null);
    stub.setCatName(name.getCat());
    return stub;
  }

  private SearchQuery.Mode toQueryMode(SearchMode mode) {
    return SearchQuery.Mode.valueOf(mode.name());
  }

  public record SearchTablesResult(TableSearchResult result, boolean success)
      implements Result<TableSearchResponse> {

    @Override
    public boolean success() {
      return success;
    }

    @Override
    public TableSearchResponse transform() {
      if (!success) {
        return null;
      }
      List<org.apache.hadoop.hive.metastore.api.TableSearchHit> hits
          = new ArrayList<>(result.hits().size());
      for (TableSearchHit hit : result.hits()) {
        hits.add(toThrift(hit));
      }
      return new TableSearchResponse(hits, result.total(), result.processedEventId());
    }

    private org.apache.hadoop.hive.metastore.api.TableSearchHit toThrift(TableSearchHit hit) {
      TableName name = hit.name();
      org.apache.hadoop.hive.metastore.api.TableSearchHit thriftHit =
          new org.apache.hadoop.hive.metastore.api.TableSearchHit(
              name.getCat(), name.getDb(), name.getTable(), hit.score());
      if (hit.table() != null) {
        thriftHit.setTable(hit.table());
      }
      return thriftHit;
    }
  }
}
