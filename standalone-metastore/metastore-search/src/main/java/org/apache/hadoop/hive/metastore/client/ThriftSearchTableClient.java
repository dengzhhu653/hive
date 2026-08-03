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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.SearchTable;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SearchTablesRequest;
import org.apache.hadoop.hive.metastore.api.TableSearchResponse;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;

/** Thrift connection pinned to a single metastore URI. */
final class ThriftSearchTableClient extends ThriftHiveMetaStoreClient implements SearchTable {

  ThriftSearchTableClient(Configuration conf) throws MetaException {
    super(conf, false);
  }

  @Override
  public TableSearchResponse searchTables(SearchTablesRequest request) throws TException {
    return client.search_tables_req(request);
  }

  boolean isActive() {
    TTransport transport = getTTransport();
    return transport != null && transport.isOpen();
  }
}
