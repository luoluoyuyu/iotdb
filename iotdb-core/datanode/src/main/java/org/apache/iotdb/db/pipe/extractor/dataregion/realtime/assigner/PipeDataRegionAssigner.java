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

package org.apache.iotdb.db.pipe.extractor.dataregion.realtime.assigner;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.ProgressReportEvent;
import org.apache.iotdb.commons.pipe.metric.PipeEventCounter;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResource;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResourceManager;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEventFactory;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.PipeRealtimeDataRegionExtractor;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.matcher.CachedSchemaPatternMatcher;
import org.apache.iotdb.db.pipe.extractor.dataregion.realtime.matcher.PipeDataRegionMatcher;
import org.apache.iotdb.db.pipe.metric.PipeAssignerMetrics;
import org.apache.iotdb.db.pipe.metric.PipeDataRegionEventCounter;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.TsFileSequenceReaderTimeseriesMetadataIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class PipeDataRegionAssigner implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeDataRegionAssigner.class);

  private static final int nonForwardingEventsProgressReportInterval =
      PipeConfig.getInstance().getPipeNonForwardingEventsProgressReportInterval();

  /**
   * The {@link PipeDataRegionMatcher} is used to match the event with the extractor based on the
   * pattern.
   */
  private final PipeDataRegionMatcher matcher;

  /** The {@link DisruptorQueue} is used to assign the event to the extractor. */
  private final DisruptorQueue disruptor;

  private final String dataRegionId;

  private int counter = 0;

  private final AtomicReference<ProgressIndex> maxProgressIndexForTsFileInsertionEvent =
      new AtomicReference<>(MinimumProgressIndex.INSTANCE);

  private final PipeEventCounter eventCounter = new PipeDataRegionEventCounter();

  public String getDataRegionId() {
    return dataRegionId;
  }

  public PipeDataRegionAssigner(final String dataRegionId) {
    this.matcher = new CachedSchemaPatternMatcher();
    this.disruptor = new DisruptorQueue(this::assignToExtractor, this::onAssignedHook);
    this.dataRegionId = dataRegionId;
    PipeAssignerMetrics.getInstance().register(this);
  }

  public void publishToAssign(final PipeRealtimeEvent event) {
    if (!event.increaseReferenceCount(PipeDataRegionAssigner.class.getName())) {
      LOGGER.warn(
          "The reference count of the realtime event {} cannot be increased, skipping it.", event);
      return;
    }

    final EnrichedEvent innerEvent = event.getEvent();
    eventCounter.increaseEventCount(innerEvent);
    if (innerEvent instanceof PipeHeartbeatEvent) {
      ((PipeHeartbeatEvent) innerEvent).onPublished();
    }

    if (!disruptor.isClosed()) {
      disruptor.publish(event);
    } else {
      onAssignedHook(event);
    }
  }

  private void onAssignedHook(final PipeRealtimeEvent realtimeEvent) {
    realtimeEvent.gcSchemaInfo();
    realtimeEvent.decreaseReferenceCount(PipeDataRegionAssigner.class.getName(), false);

    final EnrichedEvent innerEvent = realtimeEvent.getEvent();
    eventCounter.decreaseEventCount(innerEvent);
    if (innerEvent instanceof PipeHeartbeatEvent) {
      ((PipeHeartbeatEvent) innerEvent).onAssigned();
    }
  }

  private void assignToExtractor(
      final PipeRealtimeEvent event, final long sequence, final boolean endOfBatch) {
    if (disruptor.isClosed()) {
      return;
    }

    matcher
        .match(event)
        .forEach(
            extractor -> {
              if (disruptor.isClosed()) {
                return;
              }

              if (event.getEvent().isGeneratedByPipe() && !extractor.isForwardingPipeRequests()) {
                // The frequency of progress reports is limited by the counter, while progress
                // reports to TsFileInsertionEvent are not limited.
                if (!(event.getEvent() instanceof TsFileInsertionEvent)) {
                  if (counter < nonForwardingEventsProgressReportInterval) {
                    counter++;
                    return;
                  }
                  counter = 0;
                }

                final ProgressReportEvent reportEvent =
                    new ProgressReportEvent(
                        extractor.getPipeName(),
                        extractor.getCreationTime(),
                        extractor.getPipeTaskMeta(),
                        extractor.getTreePattern(),
                        extractor.getTablePattern(),
                        extractor.getRealtimeDataExtractionStartTime(),
                        extractor.getRealtimeDataExtractionEndTime());
                reportEvent.bindProgressIndex(event.getProgressIndex());
                if (!reportEvent.increaseReferenceCount(PipeDataRegionAssigner.class.getName())) {
                  LOGGER.warn(
                      "The reference count of the event {} cannot be increased, skipping it.",
                      reportEvent);
                  return;
                }
                extractor.extract(PipeRealtimeEventFactory.createRealtimeEvent(reportEvent));
                return;
              }

              final PipeRealtimeEvent copiedEvent =
                  event.shallowCopySelfAndBindPipeTaskMetaForProgressReport(
                      extractor.getPipeName(),
                      extractor.getCreationTime(),
                      extractor.getPipeTaskMeta(),
                      extractor.getTreePattern(),
                      extractor.getTablePattern(),
                      extractor.getRealtimeDataExtractionStartTime(),
                      extractor.getRealtimeDataExtractionEndTime());
              final EnrichedEvent innerEvent = copiedEvent.getEvent();

              if (innerEvent instanceof PipeTsFileInsertionEvent) {
                final PipeTsFileInsertionEvent tsFileInsertionEvent =
                    (PipeTsFileInsertionEvent) innerEvent;
                tsFileInsertionEvent.disableMod4NonTransferPipes(
                    extractor.isShouldTransferModFile());
                bindOrUpdateProgressIndexForTsFileInsertionEvent(tsFileInsertionEvent);
              }

              if (innerEvent instanceof PipeTsFileInsertionEvent
                  && (((PipeTsFileInsertionEvent) innerEvent).isTableModelEvent())) {
                try (final TsFileSequenceReader reader =
                    new TsFileSequenceReader(
                        (((PipeTsFileInsertionEvent) innerEvent).getTsFile()).getAbsolutePath())) {
                  final TsFileSequenceReaderTimeseriesMetadataIterator timeseriesMetadataIterator =
                      new TsFileSequenceReaderTimeseriesMetadataIterator(reader, true, 1);
                  while (timeseriesMetadataIterator.hasNext()) {
                    final Map<IDeviceID, List<TimeseriesMetadata>> device2TimeseriesMetadata =
                        timeseriesMetadataIterator.next();

                    for (IDeviceID deviceId : device2TimeseriesMetadata.keySet()) {
                      LOGGER.warn(
                          "{} assigner realtime load tsfile println device {}",
                          extractor.getPipeName(),
                          deviceId);
                    }
                  }
                } catch (Exception e) {
                  LOGGER.error(e.getMessage());
                }
              } else if (event.getEvent() instanceof PipeInsertNodeTabletInsertionEvent) {
                try {
                  PipeInsertNodeTabletInsertionEvent tabletInsertionEvent =
                      (PipeInsertNodeTabletInsertionEvent) event.getEvent();
                  final InsertNode insertNode = tabletInsertionEvent.getInsertNode();
                  if (insertNode instanceof RelationalInsertTabletNode) {
                    for (int i = 0;
                        i < ((RelationalInsertTabletNode) insertNode).getRowCount();
                        i++) {
                      LOGGER.warn(
                          "{} assigner realtime insertNode println device {}",
                          extractor.getPipeName(),
                          ((RelationalInsertTabletNode) insertNode).getDeviceID(i));
                    }
                  } else if (insertNode instanceof RelationalInsertRowsNode) {
                    for (InsertRowNode rowNode :
                        ((RelationalInsertRowsNode) insertNode).getInsertRowNodeList()) {
                      LOGGER.warn(
                          "{} assigner realtime  insertNode println device {}",
                          extractor.getPipeName(),
                          ((RelationalInsertRowNode) rowNode).getDeviceID());
                    }
                  } else if (insertNode instanceof RelationalInsertRowNode) {
                    LOGGER.warn(
                        "{} assigner realtime  insertNode println device {}",
                        extractor.getPipeName(),
                        ((RelationalInsertRowNode) insertNode).getDeviceID());
                  }
                } catch (Exception e) {
                  LOGGER.error(e.getMessage());
                }
              }

              if (innerEvent instanceof PipeDeleteDataNodeEvent) {
                final PipeDeleteDataNodeEvent deleteDataNodeEvent =
                    (PipeDeleteDataNodeEvent) innerEvent;
                final DeletionResourceManager manager =
                    DeletionResourceManager.getInstance(extractor.getDataRegionId());
                // increase deletion resource's reference and bind real deleteEvent
                if (Objects.nonNull(manager)
                    && DeletionResource.isDeleteNodeGeneratedInLocalByIoTV2(
                        deleteDataNodeEvent.getDeleteDataNode())) {
                  deleteDataNodeEvent.setDeletionResource(
                      manager.getDeletionResource(
                          ((PipeDeleteDataNodeEvent) event.getEvent()).getDeleteDataNode()));
                }
              }

              if (!copiedEvent.increaseReferenceCount(PipeDataRegionAssigner.class.getName())) {
                LOGGER.warn(
                    "The reference count of the event {} cannot be increased, skipping it.",
                    copiedEvent);
                return;
              }
              extractor.extract(copiedEvent);
            });
  }

  private void bindOrUpdateProgressIndexForTsFileInsertionEvent(
      final PipeTsFileInsertionEvent event) {
    if (PipeTimePartitionProgressIndexKeeper.getInstance()
        .isProgressIndexAfterOrEquals(
            dataRegionId, event.getTimePartitionId(), event.getProgressIndex())) {
      event.bindProgressIndex(maxProgressIndexForTsFileInsertionEvent.get());
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Data region {} bind {} to event {} because it was flushed prematurely.",
            dataRegionId,
            maxProgressIndexForTsFileInsertionEvent,
            event.coreReportMessage());
      }
    } else {
      maxProgressIndexForTsFileInsertionEvent.updateAndGet(
          index -> index.updateToMinimumEqualOrIsAfterProgressIndex(event.getProgressIndex()));
    }
  }

  public void startAssignTo(final PipeRealtimeDataRegionExtractor extractor) {
    matcher.register(extractor);
  }

  public void stopAssignTo(final PipeRealtimeDataRegionExtractor extractor) {
    matcher.deregister(extractor);
  }

  public boolean notMoreExtractorNeededToBeAssigned() {
    return matcher.getRegisterCount() == 0;
  }

  /**
   * Clear the matcher and disruptor. The method {@link PipeDataRegionAssigner#publishToAssign}
   * should not be used after calling this method.
   */
  @Override
  public void close() {
    PipeAssignerMetrics.getInstance().deregister(dataRegionId);
    matcher.clear();

    final long startTime = System.currentTimeMillis();
    disruptor.shutdown();
    LOGGER.info(
        "Pipe: Assigner on data region {} shutdown internal disruptor within {} ms",
        dataRegionId,
        System.currentTimeMillis() - startTime);
  }

  public int getTabletInsertionEventCount() {
    return eventCounter.getTabletInsertionEventCount();
  }

  public int getTsFileInsertionEventCount() {
    return eventCounter.getTsFileInsertionEventCount();
  }

  public int getPipeHeartbeatEventCount() {
    return eventCounter.getPipeHeartbeatEventCount();
  }
}
