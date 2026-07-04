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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.search.mapping.IndexMapping;
import org.apache.hive.search.mapping.TableDocument;
import org.apache.hive.search.mapping.field.Field;
import org.apache.hive.search.mapping.field.IdField;
import org.apache.hive.search.mapping.field.TextField;

public final class MetastoreTableMapper {
  public static final String FIELD_DB = "db";
  public static final String FIELD_TABLE = "table_name";
  public static final String FIELD_OWNER = "owner";
  public static final String FIELD_TABLE_TYPE = "table_type";
  public static final String FIELD_LOCATION = "location";
  public static final String FIELD_COMMENT = "comment";
  public static final String FIELD_COLUMNS = "columns";
  public static final String FIELD_SEARCH_TEXT = "search_text";
  // @TODO add a field storing the json format of the table

  private MetastoreTableMapper() {}

  public static String tableId(String catalog, String db, String table) {
    return catalog + "." + db + "." + table;
  }

  public static String tableId(TableName tableName) {
    return tableId(tableName.getCat(), tableName.getDb(), tableName.getTable());
  }

  public static TableDocument fromTable(Table table, IndexMapping indexMapping) {
    String db = table.getDbName();
    String name = table.getTableName();
    String catalog = table.getCatName();
    String id = tableId(catalog, db, name);
    String owner = nullToEmpty(table.getOwner());
    String tableType = nullToEmpty(table.getTableType());
    String location =
        table.getSd() == null ? "" : nullToEmpty(table.getSd().getLocation());
    String comment = "";
    if (table.getParameters() != null && table.getParameters().get("comment") != null) {
      comment = nullToEmpty(table.getParameters().get("comment"));
    }
    String columns = formatColumns(table);
    String searchText =
        String.join(
            " ",
            List.of(db, name, owner, tableType, location, comment, columns).stream()
                .filter(s -> !s.isEmpty())
                .toList());

    List<Field> fields = new ArrayList<>();
    fields.add(new TextField(FIELD_DB, db));
    fields.add(new TextField(FIELD_TABLE, name));
    fields.add(new TextField(FIELD_OWNER, owner));
    fields.add(new TextField(FIELD_TABLE_TYPE, tableType));
    fields.add(new TextField(FIELD_LOCATION, location));
    fields.add(new TextField(FIELD_COMMENT, comment));
    fields.add(new TextField(FIELD_COLUMNS, columns));
    fields.add(new TextField(FIELD_SEARCH_TEXT, searchText));
    return new TableDocument(new IdField("_id", id), fields, indexMapping);
  }

  private static String formatColumns(Table table) {
    if (table.getSd() == null || table.getSd().getCols() == null) {
      return "";
    }
    List<String> parts = new ArrayList<>();
    for (FieldSchema column : table.getSd().getCols()) {
      parts.add(formatColumn(column));
    }
    return String.join("; ", parts);
  }

  private static String formatColumn(FieldSchema column) {
    String base = column.getName() + " " + column.getType();
    if (StringUtils.isNotEmpty(column.getComment())) {
      return base + " " + column.getComment();
    }
    return base;
  }

  private static String nullToEmpty(String value) {
    return value == null ? "" : value;
  }
}
