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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.MetaStoreFilterHook;
import org.apache.hadoop.hive.metastore.api.SearchMode;
import org.apache.hadoop.hive.metastore.api.SearchTablesRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableSearchResponse;
import org.apache.hive.search.search.SearchBackend;
import org.apache.hive.search.search.SearchQuery;
import org.apache.hive.search.search.TableSearchHit;
import org.apache.hive.search.search.TableSearchResult;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(MetastoreUnitTest.class)
public class TestSearchTablesHandler {
  private final Configuration conf = new Configuration(false);

  @After
  public void tearDown() throws Exception {
    SearchProvider.reset();
  }

  @Test
  public void searchTablesReturnsThriftResponse() throws Exception {
    StubSearchBackend backend = new StubSearchBackend();
    SearchProvider.install(conf, backend);

    SearchTablesRequest request = new SearchTablesRequest(SearchMode.MATCH, Map.of("query", "sales"));
    request.setLimit(5);
    SearchTablesHandler handler = new SearchTablesHandler(mockHandler(), request);

    SearchTablesHandler.SearchTablesResult result = handler.getResult();

    TableSearchResponse response = result.transform();
    assertEquals(1, response.getHitsSize());
    assertEquals("orders", response.getHits().get(0).getTableName());
    assertEquals(1L, response.getTotal());
    assertEquals(12L, response.getProcessedEventId());
    assertNotNull(response.getHits().get(0).getTable());
  }

  @Test
  public void searchTablesFiltersInaccessibleTables() throws Exception {
    Table allowedTable = table("orders", "sales");
    Table denied = table("secret", "sales");
    SearchProvider.install(conf, new StubSearchBackend(List.of(
        new TableSearchHit(TableName.fromString("hive.sales.orders", "hive", "sales"), allowedTable, 1.0f),
        new TableSearchHit(TableName.fromString("hive.sales.secret", "hive", "sales"), denied, 0.5f))));

    MetaStoreFilterHook filterHook = mock(MetaStoreFilterHook.class);
    when(filterHook.filterTables(org.mockito.ArgumentMatchers.anyList())).thenAnswer(invocation -> {
      List<Table> tables = invocation.getArgument(0);
      List<Table> filtered = new ArrayList<>(tables.size());
      for (Table table : tables) {
        if (!"secret".equals(table.getTableName())) {
          filtered.add(table);
        }
      }
      return filtered;
    });
    org.apache.hadoop.hive.metastore.IHMSHandler handler = mockHandler(filterHook);

    SearchTablesRequest request = new SearchTablesRequest(SearchMode.MATCH, Map.of("query", "sales"));
    SearchTablesHandler searchHandler = new SearchTablesHandler(handler, request);

    SearchTablesHandler.SearchTablesResult result = searchHandler.getResult();

    TableSearchResponse response = result.transform();
    assertEquals(1, response.getHitsSize());
    assertEquals("orders", response.getHits().get(0).getTableName());
    assertEquals(1L, response.getTotal());
  }

  private static Table table(String tableName, String dbName) {
    Table table = new Table(tableName, dbName, "owner", 0, 0, 0, null, null, null, null, null, null);
    table.setCatName("hive");
    return table;
  }

  private org.apache.hadoop.hive.metastore.IHMSHandler mockHandler() {
    return mockHandler(null);
  }

  private org.apache.hadoop.hive.metastore.IHMSHandler mockHandler(MetaStoreFilterHook filterHook) {
    org.apache.hadoop.hive.metastore.IHMSHandler handler =
        mock(org.apache.hadoop.hive.metastore.IHMSHandler.class);
    when(handler.getConf()).thenReturn(conf);
    when(handler.getMetaFilterHook()).thenReturn(filterHook);
    return handler;
  }

  private static final class StubSearchBackend implements SearchBackend {
    private final TableSearchResult result;

    StubSearchBackend() {
      this(List.of(new TableSearchHit(
          TableName.fromString("hive.sales.orders", "hive", "sales"),
          table("orders", "sales"),
          1.0f)));
    }

    StubSearchBackend(List<TableSearchHit> hits) {
      this.result = new TableSearchResult(hits, hits.size(), 12);
    }

    @Override
    public void initialize(Configuration configuration) {
    }

    @Override
    public boolean isReady() {
      return true;
    }

    @Override
    public TableSearchResult search(SearchQuery query) {
      return result;
    }

    @Override
    public TableSearchResult loadTables(List<String> tableIds) {
      return new TableSearchResult(List.of(), 0, 12);
    }

    @Override
    public void close() {
    }
  }
}
