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

package org.apache.hive.search.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.search.mapping.IndexMapping;
import org.apache.hive.search.mapping.TableDocument;
import org.apache.hive.search.mapping.field.Field;
import org.apache.hive.search.mapping.field.IdField;
import org.apache.hive.search.mapping.field.TextField;
import org.apache.lucene.document.Document;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(MetastoreUnitTest.class)
public class TestMetastoreTableMapper {

  @Test
  public void tableIdIncludesCatalogDbAndTable() {
    assertEquals("hive.default.orders",
        MetastoreTableMapper.tableId("hive", "default", "orders"));
    assertEquals("hive.default.orders",
        MetastoreTableMapper.tableId(TableName.fromString("default.orders", "hive", "default")));
  }

  @Test
  public void fromTableBuildsSearchTextAndStoredFields() throws Exception {
    Configuration conf = new Configuration(false);
    IndexMapping mapping = MetastoreSchemas.defaultHiveTablesMapping("hive_tables", "bge-small", conf);

    Table table = new Table();
    table.setCatName("hive");
    table.setDbName("sales");
    table.setTableName("orders");
    table.setOwner("alice");
    table.setTableType("MANAGED_TABLE");
    table.setSd(new StorageDescriptor());
    table.getSd().setLocation("hdfs://warehouse/orders");
    table.getSd().setCols(List.of(
        new FieldSchema("id", "bigint", "order id"),
        new FieldSchema("amount", "double", null)));
    Map<String, String> params = new HashMap<>();
    params.put("comment", "daily orders");
    table.setParameters(params);

    TableDocument document = MetastoreTableMapper.fromTable(table, mapping);
    document = withSearchTextEmbedding(document, mapping, new float[] {0.1f, 0.2f, 0.3f});
    assertEquals("hive.sales.orders", document.idField().value());

    List<Document> luceneDocs = document.toDocuments();
    assertEquals(1, luceneDocs.size());
    Document luceneDoc = luceneDocs.get(0);
    assertTrue(luceneDoc.get("_id").contains("hive.sales.orders"));
    assertEquals("sales", luceneDoc.get(MetastoreTableMapper.FIELD_DB));
    assertEquals("orders", luceneDoc.get(MetastoreTableMapper.FIELD_TABLE));
    assertTrue(luceneDoc.get(MetastoreTableMapper.FIELD_SEARCH_TEXT).contains("daily orders"));
  }

  @Test
  public void semanticFieldRequiresEmbedding() throws Exception {
    Configuration conf = new Configuration(false);
    IndexMapping mapping = MetastoreSchemas.defaultHiveTablesMapping("hive_tables", "bge-small", conf);
    TableDocument document = MetastoreTableMapper.fromTable(sampleTable(), mapping);
    document.appendField(new TextField(MetastoreTableMapper.FIELD_SEARCH_TEXT, "sales data"));
    try {
      document.toDocuments();
      org.junit.Assert.fail("expected semantic field without embedding to fail");
    } catch (org.apache.hive.search.exception.IndexException expected) {
      assertTrue(expected.getMessage().contains("requires embedding"));
    }
  }

  private static Table sampleTable() {
    Table table = new Table();
    table.setCatName("hive");
    table.setDbName("default");
    table.setTableName("t");
    table.setSd(new StorageDescriptor());
    return table;
  }

  private static TableDocument withSearchTextEmbedding(
      TableDocument document, IndexMapping mapping, float[] embedding) {
    java.util.List<Field> fields = new java.util.ArrayList<>();
    for (Field field : document.fields()) {
      if (field instanceof IdField) {
        continue;
      }
      if (field instanceof TextField textField
          && MetastoreTableMapper.FIELD_SEARCH_TEXT.equals(textField.name())) {
        fields.add(textField.withEmbedding(embedding));
      } else {
        fields.add(field);
      }
    }
    return new TableDocument(document.idField(), fields, mapping);
  }
}
