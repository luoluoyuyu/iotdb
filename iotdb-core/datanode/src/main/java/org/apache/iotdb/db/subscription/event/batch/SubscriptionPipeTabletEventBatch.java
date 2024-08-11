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

package org.apache.iotdb.db.subscription.event.batch;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeLastPointTabletEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.processor.downsampling.PartialPathLastObjectCache;
import org.apache.iotdb.db.pipe.processor.downsampling.lastpoint.LastPointFilter;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingTabletQueue;
import org.apache.iotdb.db.subscription.event.SubscriptionEvent;
import org.apache.iotdb.db.subscription.event.pipe.SubscriptionPipeTabletBatchEvents;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionCommitContext;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponse;
import org.apache.iotdb.rpc.subscription.payload.poll.SubscriptionPollResponseType;
import org.apache.iotdb.rpc.subscription.payload.poll.TabletsPayload;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class SubscriptionPipeTabletEventBatch {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SubscriptionPipeTabletEventBatch.class);

  private final SubscriptionPrefetchingTabletQueue prefetchingQueue;

  private final List<EnrichedEvent> enrichedEvents = new ArrayList<>();
  private final List<Tablet> tablets = new ArrayList<>();

  private final int maxDelayInMs;
  private long firstEventProcessingTime = Long.MIN_VALUE;

  private final long maxBatchSizeInBytes;
  private long totalBufferSize = 0;

  private boolean isSealed = false;

  public SubscriptionPipeTabletEventBatch(
      final SubscriptionPrefetchingTabletQueue prefetchingQueue,
      final int maxDelayInMs,
      final long maxBatchSizeInBytes) {
    this.prefetchingQueue = prefetchingQueue;
    this.maxDelayInMs = maxDelayInMs;
    this.maxBatchSizeInBytes = maxBatchSizeInBytes;
  }

  public synchronized List<SubscriptionEvent> onEvent(@Nullable final EnrichedEvent event) {
    if (isSealed) {
      return Collections.emptyList();
    }
    if (Objects.nonNull(event)) {
      constructBatch(event);
    }
    if (shouldEmit()) {
      final List<SubscriptionEvent> events = generateSubscriptionEvents();
      isSealed = true;
      return events;
    }
    return Collections.emptyList();
  }

  public synchronized void ack() {
    for (final EnrichedEvent enrichedEvent : enrichedEvents) {
      enrichedEvent.decreaseReferenceCount(this.getClass().getName(), true);
    }
  }

  public synchronized void cleanup() {
    // clear the reference count of events
    for (final EnrichedEvent enrichedEvent : enrichedEvents) {
      enrichedEvent.clearReferenceCount(this.getClass().getName());
    }
  }

  private List<SubscriptionEvent> generateSubscriptionEvents() {
    final SubscriptionCommitContext commitContext =
        prefetchingQueue.generateSubscriptionCommitContext();
    return Collections.singletonList(
        new SubscriptionEvent(
            new SubscriptionPipeTabletBatchEvents(this),
            new SubscriptionPollResponse(
                SubscriptionPollResponseType.TABLETS.getType(),
                new TabletsPayload(tablets),
                commitContext)));
  }

  private void constructBatch(final EnrichedEvent event) {
    if (event instanceof TabletInsertionEvent) {
      final List<Tablet> currentTablets = convertToTablets((TabletInsertionEvent) event);
      if (currentTablets.isEmpty()) {
        return;
      }
      tablets.addAll(currentTablets);
      totalBufferSize +=
          currentTablets.stream()
              .map(PipeMemoryWeightUtil::calculateTabletSizeInBytes)
              .reduce(Long::sum)
              .orElse(0L);
      enrichedEvents.add(event);
      if (firstEventProcessingTime == Long.MIN_VALUE) {
        firstEventProcessingTime = System.currentTimeMillis();
      }
    } else if (event instanceof PipeTsFileInsertionEvent) {
      for (final TabletInsertionEvent tabletInsertionEvent :
          ((PipeTsFileInsertionEvent) event).toTabletInsertionEvents()) {
        final List<Tablet> currentTablets = convertToTablets(tabletInsertionEvent);
        if (Objects.isNull(currentTablets)) {
          continue;
        }
        tablets.addAll(currentTablets);
        totalBufferSize +=
            currentTablets.stream()
                .map(PipeMemoryWeightUtil::calculateTabletSizeInBytes)
                .reduce(Long::sum)
                .orElse(0L);
      }
      enrichedEvents.add(event);
      if (firstEventProcessingTime == Long.MIN_VALUE) {
        firstEventProcessingTime = System.currentTimeMillis();
      }
    } else if (event instanceof PipeLastPointTabletEvent) {
      final PipeLastPointTabletEvent lastPointTabletEvent = (PipeLastPointTabletEvent) event;

      final Tablet tablet = lastPointTabletEvent.getTablet();
      if (Objects.isNull(tablet)) {
        return;
      }
      if (lastPointFilter(tablet, lastPointTabletEvent.getPartialPathToLatestTimeCache())) {
        return;
      }
      tablets.add(tablet);
      // tablet size in bytes
      totalBufferSize += PipeMemoryWeightUtil.calculateTabletSizeInBytes(tablet);
    }
  }

  private boolean shouldEmit() {
    return totalBufferSize >= maxBatchSizeInBytes
        || System.currentTimeMillis() - firstEventProcessingTime >= maxDelayInMs;
  }

  private List<Tablet> convertToTablets(final TabletInsertionEvent tabletInsertionEvent) {
    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      return ((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent).convertToTablets();
    } else if (tabletInsertionEvent instanceof PipeRawTabletInsertionEvent) {
      return Collections.singletonList(
          ((PipeRawTabletInsertionEvent) tabletInsertionEvent).convertToTablet());
    }

    LOGGER.warn(
        "SubscriptionPipeTabletEventBatch {} only support convert PipeInsertNodeTabletInsertionEvent or PipeRawTabletInsertionEvent to tablet. Ignore {}.",
        this,
        tabletInsertionEvent);
    return Collections.emptyList();
  }

  /////////////////////////////// filter ///////////////////////////////

  private boolean lastPointFilter(
      Tablet tablet, PartialPathLastObjectCache<LastPointFilter<?>> cache) {
    String deviceId = tablet.getDeviceId();
    Object[] dataValues = tablet.values;
    int columnCount = dataValues.length;
    BitMap columnFilterMap = new BitMap(columnCount);

    for (int i = 0; i < columnCount; i++) {
      BitMap fieldBitMap = tablet.bitMaps[columnCount];

      if (fieldBitMap.isMarked(0)) {
        columnFilterMap.mark(i);
        continue;
      }

      IMeasurementSchema schema = tablet.getSchemas().get(i);
      String measurementPath = deviceId + TsFileConstant.PATH_SEPARATOR + schema.getMeasurementId();

      LastPointFilter filter = cache.getPartialPathLastObject(measurementPath);
      if (filter != null
          && !filter.filterAndMarkAsConsumed(
              tablet.timestamps[i], getObject(dataValues[i], schema.getType()))) {
        columnFilterMap.mark(i);
        fieldBitMap.mark(0);
      }
    }

    return columnFilterMap.isAllMarked();
  }

  public Object getObject(final Object value, final TSDataType dataType) {
    switch (dataType) {
      case INT32:
        return ((int[]) value)[0];
      case DATE:
        return ((LocalDate[]) value)[0];
      case INT64:
      case TIMESTAMP:
        return ((long[]) value)[0];
      case FLOAT:
        return ((float[]) value)[0];
      case DOUBLE:
        return ((double[]) value)[0];
      case BOOLEAN:
        return ((boolean[]) value)[0];
      case TEXT:
      case BLOB:
      case STRING:
        return ((Binary[]) value)[0];
      default:
        throw new UnsupportedOperationException(
            String.format("unsupported data type %s", dataType));
    }
  }

  /////////////////////////////// stringify ///////////////////////////////

  public String toString() {
    return "SubscriptionPipeTabletEventBatch{enrichedEvents="
        + enrichedEvents.stream().map(EnrichedEvent::coreReportMessage).collect(Collectors.toList())
        + ", size of tablets="
        + tablets.size()
        + ", maxDelayInMs="
        + maxDelayInMs
        + ", firstEventProcessingTime="
        + firstEventProcessingTime
        + ", maxBatchSizeInBytes="
        + maxBatchSizeInBytes
        + ", totalBufferSize="
        + totalBufferSize
        + ", isSealed="
        + isSealed
        + "}";
  }
}
