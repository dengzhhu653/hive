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

package org.apache.hive.search.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category(MetastoreUnitTest.class)
public class TestMetastoreSearchClient {

  @After
  public void tearDown() {
    MetastoreBlacklist.shared().clear();
  }

  @Test
  public void parseMetastoreUrisAddsThriftScheme() {
    Configuration conf = new Configuration(false);
    conf.set("hive.metastore.uris", "host1:9083,thrift://host2:9084");

    URI[] uris = MetastoreSearchClient.parseMetastoreUris(conf);

    assertEquals(2, uris.length);
    assertEquals("thrift", uris[0].getScheme());
    assertEquals("host1", uris[0].getHost());
    assertEquals("host2", uris[1].getHost());
  }

  @Test
  public void sharedBlacklistBlocksUnhealthyMetastore() {
    MetastoreBlacklist blacklist = MetastoreBlacklist.shared();
    blacklist.block("thrift://bad-host:9083");

    assertTrue(blacklist.isBlocked("thrift://bad-host:9083"));
    assertFalse(blacklist.isBlocked("thrift://good-host:9083"));
  }
}
