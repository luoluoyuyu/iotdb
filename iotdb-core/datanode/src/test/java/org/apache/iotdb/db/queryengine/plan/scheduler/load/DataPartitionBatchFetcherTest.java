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

package org.apache.iotdb.db.queryengine.plan.scheduler.load;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DataPartitionBatchFetcherTest {

  private static final IDeviceID DEVICE_ID = new PlainDeviceID("root.db.d1");
  private static final String USER_NAME = "user";

  private final IPartitionFetcher fetcher = mock(IPartitionFetcher.class);
  private final DataPartition dataPartition = mock(DataPartition.class);
  private final TRegionReplicaSet replicaSet =
      new TRegionReplicaSet(
          new TConsensusGroupId(TConsensusGroupType.DataRegion, 1),
          Collections.singletonList(
              new TDataNodeLocation(
                  1,
                  new TEndPoint("127.0.0.1", 6667),
                  new TEndPoint("127.0.0.1", 6668),
                  new TEndPoint("127.0.0.1", 6669),
                  new TEndPoint("127.0.0.1", 6670),
                  new TEndPoint("127.0.0.1", 6671))));

  @Before
  public void setUp() {
    when(dataPartition.getDataRegionReplicaSetForWriting(
            ArgumentMatchers.<IDeviceID>any(), ArgumentMatchers.<TTimePartitionSlot>any()))
        .thenReturn(replicaSet);
  }

  @Test
  public void testPreferredDataNodeIdIsForwardedToTheFetcher() {
    when(fetcher.getOrCreateDataPartition(anyList(), any(), eq(5))).thenReturn(dataPartition);

    final List<TRegionReplicaSet> replicaSets =
        new DataPartitionBatchFetcher(fetcher, 5).queryDataPartition(singleSlot(), USER_NAME);

    Assert.assertEquals(Collections.singletonList(replicaSet), replicaSets);
    verify(fetcher).getOrCreateDataPartition(anyList(), any(), eq(5));
  }

  @Test
  public void testNoPreferenceIsForwardedToTheFetcher() {
    when(fetcher.getOrCreateDataPartition(anyList(), any(), eq(-1))).thenReturn(dataPartition);

    final List<TRegionReplicaSet> replicaSets =
        new DataPartitionBatchFetcher(fetcher).queryDataPartition(singleSlot(), USER_NAME);

    Assert.assertEquals(Collections.singletonList(replicaSet), replicaSets);
    verify(fetcher).getOrCreateDataPartition(anyList(), any(), eq(-1));
  }

  private static List<Pair<IDeviceID, TTimePartitionSlot>> singleSlot() {
    return Collections.singletonList(new Pair<>(DEVICE_ID, new TTimePartitionSlot(0)));
  }
}
