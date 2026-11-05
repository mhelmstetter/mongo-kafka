/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mongodb.kafka.connect.source;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;

import com.mongodb.kafka.connect.source.MongoSourceConfig.StartupConfig.StartupMode;
import com.mongodb.kafka.connect.source.statistics.JmxStatisticsManager;

final class StartedMongoSourceTaskTest {
  @Nested
  final class PrefetchHeartbeatTest {
    private StartedMongoSourceTask task;

    @AfterEach
    void tearDown() {
      if (task != null) {
        task.close();
      }
    }

    @Test
    void heartbeatUsesTheLastReturnedBatchNotTheCursorPosition() throws Exception {
      Map<String, String> properties = new HashMap<>();
      properties.put(MongoSourceConfig.POLL_PREFETCH_BATCHES_CONFIG, "1");
      properties.put(MongoSourceConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1");
      properties.put(MongoSourceConfig.HEARTBEAT_TOPIC_NAME_CONFIG, "hb");
      MongoSourceConfig cfg = new MongoSourceConfig(properties);
      SourceTaskContext context = mock(SourceTaskContext.class);
      OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
      when(offsetStorageReader.offset(any())).thenReturn(emptyMap());
      when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
      BsonDocument returned = BsonDocument.parse("{\"_data\": \"01\"}");
      BsonDocument ahead = BsonDocument.parse("{\"_data\": \"02\"}");
      MongoChangeStreamCursor<BsonDocument> cursor = cast(mock(MongoChangeStreamCursor.class));
      when(cursor.getServerCursor()).thenReturn(new ServerCursor(1L, new ServerAddress()));
      when(cursor.tryNext()).thenReturn(null);
      when(cursor.getResumeToken()).thenReturn(returned, ahead);
      ChangeStreamIterable<?> changeStreamIterable = cast(mock(ChangeStreamIterable.class));
      MongoClient client = mock(MongoClient.class);
      when(changeStreamIterable.withDocumentClass(any())).thenReturn(cast(changeStreamIterable));
      when(changeStreamIterable.cursor()).thenReturn(cast(cursor));
      when(client.watch()).thenReturn(cast(changeStreamIterable));
      task =
          new StartedMongoSourceTask(
              () -> context, cfg, client, null, new JmxStatisticsManager(false, "unknown"));
      // the fetcher has read a second batch, so the cursor is ahead of anything returned
      verify(cursor, timeout(5000).atLeast(2)).getResumeToken();

      assertEquals(returned.toJson(), nextHeartbeatToken());
      assertEquals(ahead.toJson(), nextHeartbeatToken());
    }

    private String nextHeartbeatToken() throws InterruptedException {
      long deadline = System.currentTimeMillis() + 5000;
      while (System.currentTimeMillis() < deadline) {
        Thread.sleep(2); // past the heartbeat interval
        List<SourceRecord> records = task.poll();
        if (records != null) {
          assertEquals(1, records.size());
          assertEquals("hb", records.get(0).topic());
          return (String) records.get(0).sourceOffset().get("_id");
        }
      }
      throw new AssertionError("no heartbeat within 5 s");
    }
  }

  @Nested
  final class ChangeStreamIterableOptionsTest {
    private final Map<String, String> properties = new HashMap<>();
    private StartedMongoSourceTask task;

    @BeforeEach
    void setUp() {
      properties.clear();
    }

    @AfterEach
    void tearDown() {
      if (task != null) {
        task.close();
      }
    }

    @Test
    void fullDocumentBeforeChange() {
      FullDocumentBeforeChange expected = FullDocumentBeforeChange.WHEN_AVAILABLE;
      properties.put(MongoSourceConfig.FULL_DOCUMENT_BEFORE_CHANGE_CONFIG, expected.getValue());
      MongoSourceConfig cfg = new MongoSourceConfig(properties);
      SourceTaskContext context = mock(SourceTaskContext.class);
      OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
      when(offsetStorageReader.offset(any())).thenReturn(emptyMap());
      when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
      ChangeStreamIterable<?> changeStreamIterable = cast(mock(ChangeStreamIterable.class));
      MongoClient client = mock(MongoClient.class);
      when(changeStreamIterable.withDocumentClass(any())).thenReturn(cast(changeStreamIterable));
      when(changeStreamIterable.cursor()).thenReturn(cast(mock(MongoChangeStreamCursor.class)));
      when(client.watch()).thenReturn(cast(changeStreamIterable));
      task =
          new StartedMongoSourceTask(
              () -> context, cfg, client, null, new JmxStatisticsManager(false, "unknown"));
      task.poll();
      ArgumentCaptor<FullDocumentBeforeChange> argCaptor =
          ArgumentCaptor.forClass(FullDocumentBeforeChange.class);
      verify(changeStreamIterable, atLeastOnce()).fullDocumentBeforeChange(argCaptor.capture());
      List<FullDocumentBeforeChange> capturedArgs = argCaptor.getAllValues();
      assertTrue(capturedArgs.stream().allMatch(v -> v.equals(expected)), capturedArgs::toString);
    }

    @Test
    void startAtOperationTime() {
      int expectedEpochSeconds = 123;
      BsonTimestamp expected = new BsonTimestamp(expectedEpochSeconds, 0);
      properties.put(MongoSourceConfig.STARTUP_MODE_CONFIG, StartupMode.TIMESTAMP.propertyValue());
      properties.put(
          MongoSourceConfig.STARTUP_MODE_TIMESTAMP_START_AT_OPERATION_TIME_CONFIG,
          String.valueOf(expectedEpochSeconds));
      MongoSourceConfig cfg = new MongoSourceConfig(properties);
      SourceTaskContext context = mock(SourceTaskContext.class);
      OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
      when(offsetStorageReader.offset(any())).thenReturn(emptyMap());
      when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
      ChangeStreamIterable<?> changeStreamIterable = cast(mock(ChangeStreamIterable.class));
      MongoClient client = mock(MongoClient.class);
      when(changeStreamIterable.withDocumentClass(any())).thenReturn(cast(changeStreamIterable));
      when(changeStreamIterable.cursor()).thenReturn(cast(mock(MongoChangeStreamCursor.class)));
      when(client.watch()).thenReturn(cast(changeStreamIterable));
      task =
          new StartedMongoSourceTask(
              () -> context, cfg, client, null, new JmxStatisticsManager(false, "unknown"));
      task.poll();
      ArgumentCaptor<BsonTimestamp> argCaptor = ArgumentCaptor.forClass(BsonTimestamp.class);
      verify(changeStreamIterable, atLeastOnce()).startAtOperationTime(argCaptor.capture());
      List<BsonTimestamp> capturedArgs = argCaptor.getAllValues();
      assertTrue(capturedArgs.stream().allMatch(v -> v.equals(expected)), capturedArgs::toString);
    }
  }

  @Nested
  final class MissingOffsetTest {
    private final Map<String, String> properties = new HashMap<>();
    private StartedMongoSourceTask task;

    @BeforeEach
    void setUp() {
      properties.clear();
      properties.put(MongoSourceConfig.DATABASE_CONFIG, "db");
    }

    @AfterEach
    void tearDown() {
      if (task != null) {
        task.close();
      }
    }

    @Test
    void logsWhenNoStoredOffset() {
      task = startTask(null);
      assertTrue(task.isMissingOffsetLogged());
    }

    @Test
    void logsWhenStoredOffsetIsEmpty() {
      task = startTask(emptyMap());
      assertTrue(task.isMissingOffsetLogged());
    }

    @Test
    void doesNotLogWhenOffsetExists() {
      task = startTask(singletonMap(MongoSourceTask.ID_FIELD, "{\"_data\": \"826B\"}"));
      assertFalse(task.isMissingOffsetLogged());
    }

    @Test
    void messageNamesPartitionAndStartupMode() {
      properties.put(MongoSourceConfig.CONNECTION_URI_CONFIG, "mongodb://host1:27017,host2:27017");
      String message =
          StartedMongoSourceTask.missingOffsetMessage(new MongoSourceConfig(properties));
      assertTrue(message.contains("mongodb://host1:27017,host2:27017/db"), message);
      assertTrue(message.contains("startup.mode=latest"), message);
      assertTrue(message.contains("offset.partition.name"), message);
    }

    @Test
    void messageUsesCustomPartitionName() {
      properties.put(MongoSourceConfig.OFFSET_PARTITION_NAME_CONFIG, "my-partition");
      String message =
          StartedMongoSourceTask.missingOffsetMessage(new MongoSourceConfig(properties));
      assertTrue(message.contains("my-partition"), message);
    }

    private StartedMongoSourceTask startTask(final Map<String, Object> storedOffset) {
      MongoSourceConfig cfg = new MongoSourceConfig(properties);
      SourceTaskContext context = mock(SourceTaskContext.class);
      OffsetStorageReader offsetStorageReader = mock(OffsetStorageReader.class);
      when(offsetStorageReader.offset(any())).thenReturn(storedOffset);
      when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
      ChangeStreamIterable<?> changeStreamIterable = cast(mock(ChangeStreamIterable.class));
      MongoClient client = mock(MongoClient.class);
      when(changeStreamIterable.withDocumentClass(any())).thenReturn(cast(changeStreamIterable));
      when(changeStreamIterable.cursor()).thenReturn(cast(mock(MongoChangeStreamCursor.class)));
      when(client.getDatabase(any()))
          .thenReturn(cast(mock(com.mongodb.client.MongoDatabase.class)));
      when(client.getDatabase(any()).watch()).thenReturn(cast(changeStreamIterable));
      when(client.watch()).thenReturn(cast(changeStreamIterable));
      StartedMongoSourceTask startedTask =
          new StartedMongoSourceTask(
              () -> context, cfg, client, null, new JmxStatisticsManager(false, "unknown"));
      startedTask.poll();
      return startedTask;
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T cast(final Object o) {
    return (T) o;
  }
}
