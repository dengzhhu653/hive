/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore;

import com.zaxxer.hikari.HikariDataSource;

import javax.jdo.PersistenceManagerFactory;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.datasource.HikariCPDataSourceProvider;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;

public class TestHive28788 {
  private Configuration conf;
  private static final Logger LOG = LoggerFactory.getLogger(TestHive28788.class);

  @Before
  public void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);

    MetaStoreTestUtils.setConfForStandloneMode(conf);
    setupRandomObjectStoreUrl();
    ObjectStore objectStore = new ObjectStore();
    objectStore.setConf(conf);
    HMSHandler.createDefaultCatalog(objectStore, new Warehouse(conf));
    objectStore.createDatabase(
        new DatabaseBuilder()
            .setCatalogName(DEFAULT_CATALOG_NAME)
            .setName("default")
            .setLocation("file:/test/warehouse")
            .addParam("a", "b").build(conf));
    objectStore.shutdown();
  }

  @Test
  public void testConnectionLeak() throws Exception {
    ObjectStore objectStore1 = new ObjectStore();
    ObjectStore objectStore2 = new ObjectStore();
    objectStore1.setConf(conf);
    objectStore2.setConf(conf);
    PersistenceManagerFactory pmf = PersistenceManagerProvider.getPmf(false);
    HikariDataSource secondaryPool = (HikariDataSource) pmf.getConnectionFactory2();
    Assert.assertEquals(0, secondaryPool.getHikariPoolMXBean().getActiveConnections());
    Assert.assertEquals(2, secondaryPool.getHikariPoolMXBean().getIdleConnections());

    getAndCreateTable(objectStore1, "tbl1");
    getAndCreateTable(objectStore2, "tbl2");

    // No idle connection in the secondary connection pool even the ObjectStore instances have been shutdown
    objectStore1.shutdown();
    objectStore2.shutdown();
    Assert.assertEquals(2, secondaryPool.getHikariPoolMXBean().getActiveConnections());
    Assert.assertEquals(0, secondaryPool.getHikariPoolMXBean().getIdleConnections());
  }

  private void setupRandomObjectStoreUrl(){
    String currentUrl = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.CONNECT_URL_KEY);
    currentUrl = currentUrl.replace(MetaStoreServerUtils.JUNIT_DATABASE_PREFIX,
        String.format("%s_%s", MetaStoreServerUtils.JUNIT_DATABASE_PREFIX, UUID.randomUUID().toString()));
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECT_URL_KEY, currentUrl);
  }

  private void getAndCreateTable(ObjectStore objectStore, String tblName) throws Exception {
    objectStore.getTable(DEFAULT_CATALOG_NAME, "default", tblName);
    HikariCPDataSourceProvider.setThrowOnCommit(true);
    try {
      objectStore.createTable(new TableBuilder()
          .setDbName("default")
          .setTableName(tblName + "1")
          .addCol("test_col1", "int")
          .addCol("test_col2", "int")
          .setLocation("file:/test/warehouse/" + tblName + "1")
          .build(conf));
      Assert.fail("This should be failed....");
    } catch (Exception e) {
      LOG.debug("Ignore this exception", e);
    }

    HikariCPDataSourceProvider.setThrowOnCommit(false);
    objectStore.createTable(new TableBuilder()
        .setDbName("default")
        .setTableName(tblName)
        .addCol("test_col1", "int")
        .addCol("test_col2", "int")
        .setLocation("file:/test/warehouse/" + tblName)
        .build(conf));
  }
}
