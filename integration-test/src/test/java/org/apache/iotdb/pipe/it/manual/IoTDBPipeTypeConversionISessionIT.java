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

package org.apache.iotdb.pipe.it.manual;

import org.apache.iotdb.commons.utils.function.CheckedTriConsumer;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.db.pipe.receiver.transform.converter.ValueConverter;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2ManualCreateSchema;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2ManualCreateSchema.class})
public class IoTDBPipeTypeConversionISessionIT extends AbstractPipeDualManualIT {
  private static final int generateDataSize = 100;

  //  @Test
  //  public void insertTablet() {
  //    prepareTypeConversionTest(
  //        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
  //          senderSession.insertTablet(tablet);
  //        },
  //        false);
  //  }
  //
  //  @Test
  //  public void insertTabletReceiveByTsFile() {
  //    prepareTypeConversionTest(
  //        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
  //          senderSession.insertTablet(tablet);
  //        },
  //        true);
  //  }

  @Test
  public void insertAlignedTablet() {
    prepareTypeConversionTest(
        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
          senderSession.insertAlignedTablet(tablet);
        },
        false);
    fail();
  }

  @Test
  public void insertAlignedTabletReceiveByTsFile() {
    prepareTypeConversionTest(
        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
          senderSession.insertAlignedTablet(tablet);
        },
        true);
    fail();
  }

  //  @Test
  //  public void insertRecordsReceiveByTsFile() {
  //    prepareTypeConversionTest(
  //        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
  //          List<Long> timestamps = getTimestampList(tablet);
  //          Pair<List<List<String>>, List<List<TSDataType>>> pair =
  //              getMeasurementSchemasAndType(tablet);
  //          List<List<Object>> values = generateTabletInsertRecordForTable(tablet);
  //          senderSession.insertRecords(
  //              getDeviceID(tablet), timestamps, pair.left, pair.right, values);
  //        },
  //        true);
  //  }
  //
  //  @Test
  //  public void insertRecord() {
  //    prepareTypeConversionTest(
  //        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
  //          List<Long> timestamps = getTimestampList(tablet);
  //          Pair<List<List<String>>, List<List<TSDataType>>> pair =
  //              getMeasurementSchemasAndType(tablet);
  //          List<List<Object>> values = generateTabletInsertRecordForTable(tablet);
  //          for (int i = 0; i < values.size(); i++) {
  //            senderSession.insertRecord(
  //                tablet.getDeviceId(),
  //                timestamps.get(i),
  //                pair.left.get(i),
  //                pair.right.get(i),
  //                values.get(i).toArray());
  //          }
  //        },
  //        false);
  //  }
  //
  //  @Test
  //  public void insertRecordReceiveByTsFile() {
  //    prepareTypeConversionTest(
  //        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
  //          List<Long> timestamps = getTimestampList(tablet);
  //          Pair<List<List<String>>, List<List<TSDataType>>> pair =
  //              getMeasurementSchemasAndType(tablet);
  //          List<List<Object>> values = generateTabletInsertRecordForTable(tablet);
  //          for (int i = 0; i < values.size(); i++) {
  //            senderSession.insertRecord(
  //                tablet.getDeviceId(),
  //                timestamps.get(i),
  //                pair.left.get(i),
  //                pair.right.get(i),
  //                values.get(i).toArray());
  //          }
  //        },
  //        true);
  //  }
  //
  //  @Test
  //  public void insertAlignedRecord() {
  //    prepareTypeConversionTest(
  //        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
  //          List<Long> timestamps = getTimestampList(tablet);
  //          Pair<List<List<String>>, List<List<TSDataType>>> pair =
  //              getMeasurementSchemasAndType(tablet);
  //          List<List<Object>> values = generateTabletInsertRecordForTable(tablet);
  //          for (int i = 0; i < values.size(); i++) {
  //            senderSession.insertAlignedRecord(
  //                tablet.getDeviceId(),
  //                timestamps.get(i),
  //                pair.left.get(i),
  //                pair.right.get(i),
  //                values.get(i));
  //          }
  //        },
  //        false);
  //  }
  //
  //  @Test
  //  public void insertAlignedRecordReceiveByTsFile() {
  //    prepareTypeConversionTest(
  //        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
  //          List<Long> timestamps = getTimestampList(tablet);
  //          Pair<List<List<String>>, List<List<TSDataType>>> pair =
  //              getMeasurementSchemasAndType(tablet);
  //          List<List<Object>> values = generateTabletInsertRecordForTable(tablet);
  //          for (int i = 0; i < values.size(); i++) {
  //            senderSession.insertAlignedRecord(
  //                tablet.getDeviceId(),
  //                timestamps.get(i),
  //                pair.left.get(i),
  //                pair.right.get(i),
  //                values.get(i));
  //          }
  //        },
  //        true);
  //  }
  //
  //  @Test
  //  public void insertRecords() {
  //    prepareTypeConversionTest(
  //        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
  //          List<Long> timestamps = getTimestampList(tablet);
  //          Pair<List<List<String>>, List<List<TSDataType>>> pair =
  //              getMeasurementSchemasAndType(tablet);
  //          List<List<Object>> values = generateTabletInsertRecordForTable(tablet);
  //          senderSession.insertRecords(
  //              getDeviceID(tablet), timestamps, pair.left, pair.right, values);
  //        },
  //        false);
  //  }
  //
  //  @Test
  //  public void insertAlignedRecords() {
  //    prepareTypeConversionTest(
  //        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
  //          List<Long> timestamps = getTimestampList(tablet);
  //          Pair<List<List<String>>, List<List<TSDataType>>> pair =
  //              getMeasurementSchemasAndType(tablet);
  //          List<List<Object>> values = generateTabletInsertRecordForTable(tablet);
  //          senderSession.insertAlignedRecords(
  //              getDeviceID(tablet), timestamps, pair.left, pair.right, values);
  //        },
  //        false);
  //  }
  //
  //  @Test
  //  public void insertAlignedRecordsReceiveByTsFile() {
  //    prepareTypeConversionTest(
  //        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
  //          List<Long> timestamps = getTimestampList(tablet);
  //          Pair<List<List<String>>, List<List<TSDataType>>> pair =
  //              getMeasurementSchemasAndType(tablet);
  //          List<List<Object>> values = generateTabletInsertRecordForTable(tablet);
  //          senderSession.insertAlignedRecords(
  //              getDeviceID(tablet), timestamps, pair.left, pair.right, values);
  //        },
  //        true);
  //  }
  //
  //  @Test
  //  public void insertStringRecordsOfOneDevice() {
  //    prepareTypeConversionTest(
  //        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
  //          List<Long> timestamps = getTimestampList(tablet);
  //          Pair<List<List<String>>, List<List<TSDataType>>> pair =
  //              getMeasurementSchemasAndType(tablet);
  //          List<List<String>> values = generateTabletInsertStrRecordForTable(tablet);
  //          senderSession.insertStringRecordsOfOneDevice(
  //              tablet.getDeviceId(), timestamps, pair.left, values);
  //        },
  //        false);
  //  }
  //
  //  @Test
  //  public void insertStringRecordsOfOneDeviceReceiveByTsFile() {
  //    prepareTypeConversionTest(
  //        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
  //          List<Long> timestamps = getTimestampList(tablet);
  //          Pair<List<List<String>>, List<List<TSDataType>>> pair =
  //              getMeasurementSchemasAndType(tablet);
  //          List<List<String>> values = generateTabletInsertStrRecordForTable(tablet);
  //          senderSession.insertStringRecordsOfOneDevice(
  //              tablet.getDeviceId(), timestamps, pair.left, values);
  //        },
  //        true);
  //  }
  //
  //  @Test
  //  public void insertAlignedStringRecordsOfOneDevice() {
  //    prepareTypeConversionTest(
  //        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
  //          List<Long> timestamps = getTimestampList(tablet);
  //          Pair<List<List<String>>, List<List<TSDataType>>> pair =
  //              getMeasurementSchemasAndType(tablet);
  //          List<List<String>> values = generateTabletInsertStrRecordForTable(tablet);
  //          senderSession.insertAlignedStringRecordsOfOneDevice(
  //              tablet.getDeviceId(), timestamps, pair.left, values);
  //        },
  //        false);
  //  }
  //
  //  @Test
  //  public void insertAlignedStringRecordsOfOneDeviceReceiveByTsFile() {
  //    prepareTypeConversionTest(
  //        (ISession senderSession, ISession receiverSession, Tablet tablet) -> {
  //          List<Long> timestamps = getTimestampList(tablet);
  //          Pair<List<List<String>>, List<List<TSDataType>>> pair =
  //              getMeasurementSchemasAndType(tablet);
  //          List<List<String>> values = generateTabletInsertStrRecordForTable(tablet);
  //          senderSession.insertAlignedStringRecordsOfOneDevice(
  //              tablet.getDeviceId(), timestamps, pair.left, values);
  //        },
  //        true);
  //  }

  private SessionDataSet query(
      ISession session, List<IMeasurementSchema> measurementSchemas, String deviceId)
      throws IoTDBConnectionException, StatementExecutionException {
    String sql = "select ";
    StringBuffer param = new StringBuffer();
    for (IMeasurementSchema schema : measurementSchemas) {
      param.append(schema.getMeasurementId());
      param.append(',');
    }
    sql = sql + param.substring(0, param.length() - 1);
    sql = sql + " from " + deviceId + " ORDER BY time ASC";
    return session.executeQueryStatement(sql);
  }

  private void prepareTypeConversionTest(
      CheckedTriConsumer<ISession, ISession, Tablet, Exception> consumer, boolean isTsFile) {
    List<Pair<MeasurementSchema, MeasurementSchema>> measurementSchemas =
        generateMeasurementSchemas();
    long timeoutSeconds = 120;
    String uuid = RandomStringUtils.random(8, true, false);
    for (Pair<MeasurementSchema, MeasurementSchema> pair : measurementSchemas) {
      createTimeSeries(
          uuid.toString(), pair.left.getMeasurementId(), pair.left.getType().name(), senderEnv);
      createTimeSeries(
          uuid.toString(), pair.right.getMeasurementId(), pair.right.getType().name(), receiverEnv);
    }

    try (ISession senderSession = senderEnv.getSessionConnection();
        ISession receiverSession = receiverEnv.getSessionConnection()) {
      Tablet tablet = generateTabletAndMeasurementSchema(measurementSchemas, "root.test." + uuid);
      if (isTsFile) {
        consumer.accept(senderSession, receiverSession, tablet);
        Thread.sleep(10000);
        createDataPipe(uuid, true);
        senderSession.executeNonQueryStatement("flush");
      } else {
        createDataPipe(uuid, false);
        Thread.sleep(10000);
        consumer.accept(senderSession, receiverSession, tablet);
        senderSession.executeNonQueryStatement("flush");
      }
      List<List<Object>> expectedValues =
          generateTabletResultSetForTable(tablet, measurementSchemas);
      System.out.println(tablet.bitMaps.length);
      System.out.println(tablet.getSchemas().size());
      await()
          .pollInSameThread()
          .pollDelay(1L, TimeUnit.SECONDS)
          .pollInterval(1L, TimeUnit.SECONDS)
          .atMost(timeoutSeconds, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                validateResultSet(
                    query(receiverSession, tablet.getSchemas(), tablet.getDeviceId()),
                    expectedValues,
                    tablet.timestamps);
              });
      senderSession.close();
      receiverSession.close();
      tablet.reset();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void createTimeSeries(String diff, String measurementID, String dataType, BaseEnv env) {
    String timeSeriesCreation =
        String.format(
            "create timeseries root.test.%s.%s with datatype=%s,encoding=PLAIN",
            diff, measurementID, dataType);
    TestUtils.tryExecuteNonQueriesWithRetry(env, Collections.singletonList(timeSeriesCreation));
  }

  private void createDataPipe(String diff, boolean isTSFile) {
    String sql =
        String.format(
            "create pipe test%s"
                + " with source ('source'='iotdb-source','source.path'='root.test.**','realtime.mode'='%s','realtime.enable'='%s','history.enable'='%s')"
                + " with processor ('processor'='do-nothing-processor')"
                + " with sink ('node-urls'='%s:%s','batch.enable'='false','sink.format'='%s')",
            diff,
            isTSFile ? "file" : "forced-log",
            !isTSFile,
            isTSFile,
            receiverEnv.getIP(),
            receiverEnv.getPort(),
            isTSFile ? "tsfile" : "tablet");
    System.out.println(sql);
    TestUtils.tryExecuteNonQueriesWithRetry(senderEnv, Collections.singletonList(sql));
  }

  private void validateResultSet(
      SessionDataSet dataSet, List<List<Object>> values, long[] timestamps)
      throws IoTDBConnectionException, StatementExecutionException {
    int index = 0;
    while (dataSet.hasNext()) {
      System.out.println();
      RowRecord record = dataSet.next();
      List<Field> fields = record.getFields();
      assertEquals(record.getTimestamp(), timestamps[index]);
      List<Object> rowValues = values.get(index++);
      assertEquals(rowValues.size(), fields.size());
      for (int i = 0; i < fields.size(); i++) {
        Field field = fields.get(i);
        System.out.print(field.getDataType() + " ");
        if (field.getDataType() == null) {
          continue;
        }
        switch (field.getDataType()) {
          case INT64:
          case TIMESTAMP:
            assertEquals(field.getLongV(), (long) rowValues.get(i));
            break;
          case DATE:
            assertEquals(field.getDateV(), rowValues.get(i));
            break;
          case BLOB:
            assertEquals(field.getBinaryV(), rowValues.get(i));
            break;
          case TEXT:
          case STRING:
            assertEquals(field.getStringValue(), rowValues.get(i));
            break;
          case INT32:
            assertEquals(field.getIntV(), (int) rowValues.get(i));
            break;
          case DOUBLE:
            assertEquals(0, Double.compare(field.getDoubleV(), (double) rowValues.get(i)));
            break;
          case FLOAT:
            assertEquals(0, Float.compare(field.getFloatV(), (float) rowValues.get(i)));
            break;
        }
      }
    }
    assertEquals(values.size(), index);
  }

  private boolean[] createTestDataForBoolean() {
    boolean[] data = new boolean[generateDataSize];
    Random random = new Random();
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextBoolean();
    }
    return data;
  }

  private int[] createTestDataForInt32() {
    int[] data = new int[generateDataSize];
    Random random = new Random();
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextInt();
    }
    return data;
  }

  private long[] createTestDataForInt64() {
    long[] data = new long[generateDataSize];
    Random random = new Random();
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextLong();
    }
    return data;
  }

  private float[] createTestDataForFloat() {
    float[] data = new float[generateDataSize];
    Random random = new Random();
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextFloat();
    }
    return data;
  }

  private double[] createTestDataForDouble() {
    double[] data = new double[generateDataSize];
    Random random = new Random();
    for (int i = 0; i < data.length; i++) {
      data[i] = random.nextDouble();
    }
    return data;
  }

  private long[] createTestDataForTimestamp() {
    long[] data = new long[generateDataSize];
    long time = new Date().getTime();
    for (int i = 0; i < data.length; i++) {
      data[i] = time + i;
    }
    return data;
  }

  private LocalDate[] createTestDataForDate() {
    LocalDate[] data = new LocalDate[generateDataSize];
    int year = 2023;
    int month = 1;
    int day = 1;
    for (int i = 0; i < data.length; i++) {
      data[i] = DateUtils.parseIntToLocalDate(year * 10000 + (month * 100) + day);
      // update
      day++;
      if (day > 28) {
        day = 1;
        month++;
        if (month > 12) {
          month = 1;
          year++;
        }
      }
    }
    return data;
  }

  private Binary[] createTestDataForString() {
    String[] stringData = {
      "Hello",
      "Hello World!",
      "This is a test.",
      "IoTDB Hello World!!!!",
      "IoTDB is an excellent time series database!!!!!!!!!"
    };
    Binary[] data = new Binary[generateDataSize];
    for (int i = 0; i < data.length; i++) {
      data[i] =
          new Binary(stringData[(i % stringData.length)].getBytes(TSFileConfig.STRING_CHARSET));
    }
    return data;
  }

  private List<Long> getTimestampList(Tablet tablet) {
    long[] timestamps = tablet.timestamps;
    List<Long> data = new ArrayList<>(timestamps.length);
    for (long timestamp : timestamps) {
      data.add(timestamp);
    }
    return data;
  }

  private Pair<List<List<String>>, List<List<TSDataType>>> getMeasurementSchemasAndType(
      Tablet tablet) {
    List<List<String>> schemaData = new ArrayList<>(tablet.rowSize);
    List<List<TSDataType>> typeData = new ArrayList<>(tablet.rowSize);
    List<String> measurementSchemas = new ArrayList<>(tablet.getSchemas().size());
    List<TSDataType> types = new ArrayList<>(tablet.rowSize);
    for (IMeasurementSchema measurementSchema : tablet.getSchemas()) {
      measurementSchemas.add(measurementSchema.getMeasurementId());
      types.add(measurementSchema.getType());
    }

    for (int i = 0; i < tablet.rowSize; i++) {
      schemaData.add(measurementSchemas);
      typeData.add(types);
    }

    return new Pair<>(schemaData, typeData);
  }

  private List<String> getDeviceID(Tablet tablet) {
    List<String> data = new ArrayList<>(tablet.rowSize);
    for (int i = 0; i < tablet.rowSize; i++) {
      data.add(tablet.getDeviceId());
    }
    return data;
  }

  private List<List<Object>> generateTabletResultSetForTable(
      final Tablet tablet, List<Pair<MeasurementSchema, MeasurementSchema>> pairs) {
    List<List<Object>> insertRecords = new ArrayList<>(tablet.rowSize);
    final List<IMeasurementSchema> schemas = tablet.getSchemas();
    final Object[] values = tablet.values;
    for (int i = 0; i < tablet.rowSize; i++) {
      List<Object> insertRecord = new ArrayList<>();
      for (int j = 0; j < schemas.size(); j++) {
        TSDataType sourceType = pairs.get(j).left.getType();
        TSDataType targetType = pairs.get(j).right.getType();
        Object value = null;
        switch (sourceType) {
          case INT64:
          case TIMESTAMP:
            value = ValueConverter.convert(sourceType, targetType, ((long[]) values[j])[i]);
            insertRecord.add(convert(value, targetType));
            break;
          case INT32:
            value = ValueConverter.convert(sourceType, targetType, ((int[]) values[j])[i]);
            insertRecord.add(convert(value, targetType));
            break;
          case DOUBLE:
            value = ValueConverter.convert(sourceType, targetType, ((double[]) values[j])[i]);
            insertRecord.add(convert(value, targetType));
            break;
          case FLOAT:
            value = ValueConverter.convert(sourceType, targetType, ((float[]) values[j])[i]);
            insertRecord.add(convert(value, targetType));
            break;
          case DATE:
            value =
                ValueConverter.convert(
                    sourceType,
                    targetType,
                    DateUtils.parseDateExpressionToInt(((LocalDate[]) values[j])[i]));
            insertRecord.add(convert(value, targetType));
            break;
          case TEXT:
          case STRING:
            value = ValueConverter.convert(sourceType, targetType, ((Binary[]) values[j])[i]);
            insertRecord.add(convert(value, targetType));
            break;
          case BLOB:
            value = ValueConverter.convert(sourceType, targetType, ((Binary[]) values[j])[i]);
            insertRecord.add(convert(value, targetType));
            break;
          case BOOLEAN:
            value = ValueConverter.convert(sourceType, targetType, ((boolean[]) values[j])[i]);
            insertRecord.add(convert(value, targetType));
            break;
        }
      }
      insertRecords.add(insertRecord);
    }

    return insertRecords;
  }

  private Object convert(Object value, TSDataType targetType) {
    switch (targetType) {
      case DATE:
        return DateUtils.parseIntToLocalDate((Integer) value);
      case TEXT:
      case STRING:
        return new String(((Binary) value).getValues(), TSFileConfig.STRING_CHARSET);
    }
    return value;
  }

  private List<List<Object>> generateTabletInsertRecordForTable(final Tablet tablet) {
    List<List<Object>> insertRecords = new ArrayList<>(tablet.rowSize);
    final List<IMeasurementSchema> schemas = tablet.getSchemas();
    final Object[] values = tablet.values;
    for (int i = 0; i < tablet.rowSize; i++) {
      List<Object> insertRecord = new ArrayList<>();
      for (int j = 0; j < schemas.size(); j++) {
        switch (schemas.get(j).getType()) {
          case INT64:
          case TIMESTAMP:
            insertRecord.add(((long[]) values[j])[i]);
            break;
          case INT32:
            insertRecord.add(((int[]) values[j])[i]);
            break;
          case DOUBLE:
            insertRecord.add(((double[]) values[j])[i]);
            break;
          case FLOAT:
            insertRecord.add(((float[]) values[j])[i]);
            break;
          case DATE:
            insertRecord.add(((LocalDate[]) values[j])[i]);
            break;
          case TEXT:
          case STRING:
            insertRecord.add(
                new String(((Binary[]) values[j])[i].getValues(), TSFileConfig.STRING_CHARSET));
            break;
          case BLOB:
            insertRecord.add(((Binary[]) values[j])[i]);
            break;
          case BOOLEAN:
            insertRecord.add(((boolean[]) values[j])[i]);
            break;
        }
      }
      insertRecords.add(insertRecord);
    }

    return insertRecords;
  }

  private List<List<String>> generateTabletInsertStrRecordForTable(Tablet tablet) {
    List<List<String>> insertRecords = new ArrayList<>(tablet.rowSize);
    final List<IMeasurementSchema> schemas = tablet.getSchemas();
    final Object[] values = tablet.values;
    for (int i = 0; i < tablet.rowSize; i++) {
      List<String> insertRecord = new ArrayList<>();
      for (int j = 0; j < schemas.size(); j++) {
        switch (schemas.get(j).getType()) {
          case INT64:
            insertRecord.add(String.valueOf(((long[]) values[j])[i]));
            break;
          case TIMESTAMP:
            insertRecord.add(
                RpcUtils.formatDatetime(
                    "default", "ms", ((long[]) values[j])[i], ZoneId.systemDefault()));
            break;
          case INT32:
            insertRecord.add(String.valueOf(((int[]) values[j])[i]));
            break;
          case DOUBLE:
            insertRecord.add(String.valueOf(((double[]) values[j])[i]));
            break;
          case FLOAT:
            insertRecord.add(String.valueOf(((float[]) values[j])[i]));
            break;
          case DATE:
            insertRecord.add(((LocalDate[]) values[j])[i].toString());
            break;
          case TEXT:
          case STRING:
            insertRecord.add(
                new String(((Binary[]) values[j])[i].getValues(), TSFileConfig.STRING_CHARSET));
            break;
          case BLOB:
            String value =
                BytesUtils.parseBlobByteArrayToString(((Binary[]) values[j])[i].getValues())
                    .substring(2);
            insertRecord.add(String.format("X'%s'", value));
            break;
          case BOOLEAN:
            insertRecord.add(String.valueOf(((boolean[]) values[j])[i]));
            break;
        }
      }
      insertRecords.add(insertRecord);
    }

    return insertRecords;
  }

  private Tablet generateTabletAndMeasurementSchema(
      List<Pair<MeasurementSchema, MeasurementSchema>> pairs, String deviceId) {
    long[] timestamp = createTestDataForTimestamp();
    Object[] objects = new Object[pairs.size()];
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>(pairs.size());
    BitMap[] bitMaps = new BitMap[pairs.size()];
    for (int i = 0; i < bitMaps.length; i++) {
      bitMaps[i] = new BitMap(generateDataSize);
    }
    List<Tablet.ColumnType> columnTypes = new ArrayList<>(pairs.size());
    for (int i = 0; i < objects.length; i++) {
      MeasurementSchema schema = pairs.get(i).left;
      measurementSchemas.add(schema);
      columnTypes.add(Tablet.ColumnType.MEASUREMENT);
      switch (schema.getType()) {
        case INT64:
          objects[i] = createTestDataForInt64();
          break;
        case INT32:
          objects[i] = createTestDataForInt32();
          break;
        case TIMESTAMP:
          objects[i] = createTestDataForTimestamp();
          break;
        case DOUBLE:
          objects[i] = createTestDataForDouble();
          break;
        case FLOAT:
          objects[i] = createTestDataForFloat();
          break;
        case DATE:
          objects[i] = createTestDataForDate();
          break;
        case STRING:
        case BLOB:
        case TEXT:
          objects[i] = createTestDataForString();
          break;
        case BOOLEAN:
          objects[i] = createTestDataForBoolean();
          break;
      }
    }
    return new Tablet(
        deviceId, measurementSchemas, columnTypes, timestamp, objects, bitMaps, generateDataSize);
  }

  private List<Pair<MeasurementSchema, MeasurementSchema>> generateMeasurementSchemas() {
    TSDataType[] dataTypes = {
      TSDataType.STRING,
      TSDataType.TEXT,
      TSDataType.BLOB,
      TSDataType.TIMESTAMP,
      TSDataType.BOOLEAN,
      TSDataType.DATE,
      TSDataType.DOUBLE,
      TSDataType.FLOAT,
      TSDataType.INT32,
      TSDataType.INT64
    };
    List<Pair<MeasurementSchema, MeasurementSchema>> pairs = new ArrayList<>();

    for (TSDataType type : dataTypes) {
      for (TSDataType dataType : dataTypes) {
        String id = String.format("%s2%s", type.name(), dataType.name());
        pairs.add(new Pair<>(new MeasurementSchema(id, type), new MeasurementSchema(id, dataType)));
      }
    }
    return pairs;
  }
}