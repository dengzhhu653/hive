/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.events;

import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.metastore.events.PreLoadPartitionDoneEvent;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthorizableEvent;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthzInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
 Authorizable Event for HiveMetaStore operation LoadPartition
 */

public class LoadPartitionDoneEvent extends HiveMetaStoreAuthorizableEvent {
  private static final Logger LOG = LoggerFactory.getLogger(LoadPartitionDoneEvent.class);

  private String COMMAND_STR = "alter table load partition";

  public LoadPartitionDoneEvent(PreEventContext preEventContext) {
    super(preEventContext);
  }

  @Override
  public HiveMetaStoreAuthzInfo getAuthzContext() {
    HiveMetaStoreAuthzInfo ret = new HiveMetaStoreAuthzInfo(preEventContext, HiveOperationType.ALTERTABLE_ADDPARTS, getInputHObjs(), getOutputHObjs(), COMMAND_STR);

    return ret;
  }

  private List<HivePrivilegeObject> getInputHObjs() { return Collections.emptyList(); }

  private List<HivePrivilegeObject> getOutputHObjs() {
    LOG.debug("==> DropTableEvent.getOutputHObjs()");

    List<HivePrivilegeObject> ret   = new ArrayList<>();
    PreLoadPartitionDoneEvent event = (PreLoadPartitionDoneEvent) preEventContext;

    ret.add(new HivePrivilegeObject(HivePrivilegeObjectType.TABLE_OR_VIEW, event.getDbName(), event.getTableName()));

    LOG.debug("<== DropTableEvent.getOutputHObjs(): ret={}", ret);

    return ret;
  }
}
