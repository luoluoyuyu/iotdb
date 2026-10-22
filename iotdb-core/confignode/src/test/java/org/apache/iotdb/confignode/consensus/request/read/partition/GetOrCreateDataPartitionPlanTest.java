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

package org.apache.iotdb.confignode.consensus.request.read.partition;

import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class GetOrCreateDataPartitionPlanTest {

  @Test
  public void testPreferredDataNodeIdDefaultsToNoPreference() {
    final TDataPartitionReq req = newEmptyRequest();
    Assert.assertFalse(req.isSetPreferredDataNodeId());

    Assert.assertEquals(
        -1,
        GetOrCreateDataPartitionPlan.convertFromRpcTDataPartitionReq(req).getPreferredDataNodeId());
  }

  @Test
  public void testPreferredDataNodeIdIsConverted() {
    final TDataPartitionReq req = newEmptyRequest();
    req.setPreferredDataNodeId(3);

    Assert.assertEquals(
        3,
        GetOrCreateDataPartitionPlan.convertFromRpcTDataPartitionReq(req).getPreferredDataNodeId());
  }

  @Test
  public void testPreferredDataNodeIdDoesNotAffectEquality() {
    final TDataPartitionReq withoutHint = newEmptyRequest();
    final TDataPartitionReq withHint = newEmptyRequest();
    withHint.setPreferredDataNodeId(3);

    final GetOrCreateDataPartitionPlan planWithoutHint =
        GetOrCreateDataPartitionPlan.convertFromRpcTDataPartitionReq(withoutHint);
    final GetOrCreateDataPartitionPlan planWithHint =
        GetOrCreateDataPartitionPlan.convertFromRpcTDataPartitionReq(withHint);

    // The hint is request-scoped metadata, not part of the logical plan identity.
    Assert.assertEquals(planWithoutHint, planWithHint);
    Assert.assertEquals(planWithoutHint.hashCode(), planWithHint.hashCode());
  }

  private static TDataPartitionReq newEmptyRequest() {
    return new TDataPartitionReq(
        new ConcurrentHashMap<String, Map<TSeriesPartitionSlot, TTimeSlotList>>());
  }
}
