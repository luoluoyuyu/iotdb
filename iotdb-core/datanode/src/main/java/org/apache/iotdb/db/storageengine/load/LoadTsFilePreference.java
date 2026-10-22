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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.load;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

/**
 * Resolves the best-effort placement preference used by LOAD when it has to create DataRegions.
 *
 * <p>See {@code load_tsfile_prefer_local_node}. The preference only applies to the DataRegions
 * created by the LOAD request itself; existing DataRegions are never moved, and the preferred
 * position can be lost after a leader election or a Region migration.
 */
public final class LoadTsFilePreference {

  private LoadTsFilePreference() {}

  /**
   * @return the local DataNode id when the preference is enabled, otherwise -1
   */
  public static int getPreferredDataNodeId() {
    final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    return config.isLoadTsFilePreferLocalNode() ? config.getDataNodeId() : -1;
  }
}
