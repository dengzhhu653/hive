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

import java.util.concurrent.ConcurrentHashMap;

/** Process-wide blacklist of metastore URIs with unhealthy search indexes. */
public final class MetastoreBlacklist {
  private static final long DEFAULT_BLOCK_TTL_MS = 30_000L;
  private static final MetastoreBlacklist SHARED = new MetastoreBlacklist(DEFAULT_BLOCK_TTL_MS);

  private final ConcurrentHashMap<String, BlockEntry> blocked = new ConcurrentHashMap<>();
  private volatile long blockTtlMs;

  public MetastoreBlacklist(long blockTtlMs) {
    this.blockTtlMs = Math.max(0L, blockTtlMs);
  }

  public static MetastoreBlacklist shared() {
    return SHARED;
  }

  public void setBlockTtlMs(long blockTtlMs) {
    this.blockTtlMs = Math.max(0L, blockTtlMs);
  }

  public long blockTtlMs() {
    return blockTtlMs;
  }

  public void block(String metastoreUri, String reason) {
    if (metastoreUri == null) {
      return;
    }
    if (blockTtlMs == 0L) {
      blocked.remove(metastoreUri);
      return;
    }
    blocked.put(metastoreUri, new BlockEntry(System.currentTimeMillis() + blockTtlMs, reason));
  }

  public boolean isBlocked(String metastoreUri) {
    return blockReason(metastoreUri) != null;
  }

  /** Returns the block reason while the URI is blacklisted, otherwise {@code null}. */
  public String blockReason(String metastoreUri) {
    if (metastoreUri == null) {
      return null;
    }
    BlockEntry entry = blocked.get(metastoreUri);
    if (entry == null) {
      return null;
    }
    if (System.currentTimeMillis() >= entry.blockedUntilMs()) {
      blocked.remove(metastoreUri, entry);
      return null;
    }
    return entry.reason();
  }

  public void clear() {
    blocked.clear();
  }

  private record BlockEntry(long blockedUntilMs, String reason) {}
}
