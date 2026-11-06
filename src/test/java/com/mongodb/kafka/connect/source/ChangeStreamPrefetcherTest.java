/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mongodb.kafka.connect.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.bson.BsonDocument;
import org.bson.BsonInt32;

import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoChangeStreamCursor;

class ChangeStreamPrefetcherTest {
  private static final long WAIT_MS = 5000;

  private static BsonDocument doc(final int i) {
    return new BsonDocument("i", new BsonInt32(i));
  }

  @SuppressWarnings("unchecked")
  private static MongoChangeStreamCursor<BsonDocument> openCursor() {
    MongoChangeStreamCursor<BsonDocument> cursor = mock(MongoChangeStreamCursor.class);
    when(cursor.getServerCursor()).thenReturn(new ServerCursor(1L, new ServerAddress()));
    return cursor;
  }

  private static ChangeStreamPrefetcher start(
      final MongoChangeStreamCursor<BsonDocument> cursor, final int capacity, final int maxBatch) {
    return new ChangeStreamPrefetcher(cursor, capacity, maxBatch, 100, ms -> {}, "test-prefetcher");
  }

  @Test
  @DisplayName("batches keep cursor order and carry the resume token as of the end of the batch")
  void batchesInOrderWithResumeToken() throws Exception {
    MongoChangeStreamCursor<BsonDocument> cursor = openCursor();
    BsonDocument token1 = doc(101);
    BsonDocument token2 = doc(102);
    when(cursor.tryNext()).thenReturn(doc(1), doc(2), doc(3), null);
    when(cursor.available()).thenReturn(1);
    when(cursor.getResumeToken()).thenReturn(token1, token2);
    try (ChangeStreamPrefetcher prefetcher = start(cursor, 1, 2)) {
      ChangeStreamPrefetcher.FetchedBatch first = prefetcher.poll(WAIT_MS);
      assertNotNull(first);
      assertEquals(2, first.getDocuments().size());
      assertEquals(doc(1), first.getDocuments().get(0));
      assertEquals(doc(2), first.getDocuments().get(1));
      assertSame(token1, first.getResumeToken());

      ChangeStreamPrefetcher.FetchedBatch second = prefetcher.poll(WAIT_MS);
      assertNotNull(second);
      assertEquals(1, second.getDocuments().size());
      assertEquals(doc(3), second.getDocuments().get(0));
      assertSame(token2, second.getResumeToken());
    }
  }

  @Test
  @DisplayName("a cursor error is queued after the batches read before it and ends the fetcher")
  void errorIsQueuedAfterEarlierBatches() throws Exception {
    MongoChangeStreamCursor<BsonDocument> cursor = openCursor();
    MongoException error = new MongoException("boom");
    when(cursor.tryNext()).thenReturn(doc(1), (BsonDocument) null).thenThrow(error);
    when(cursor.available()).thenReturn(1);
    try (ChangeStreamPrefetcher prefetcher = start(cursor, 1, 10)) {
      ChangeStreamPrefetcher.FetchedBatch first = prefetcher.poll(WAIT_MS);
      assertNotNull(first);
      assertEquals(1, first.getDocuments().size());
      assertNull(first.getError());

      ChangeStreamPrefetcher.FetchedBatch failed = prefetcher.poll(WAIT_MS);
      assertNotNull(failed);
      assertSame(error, failed.getError());
      verify(cursor, timeout(WAIT_MS)).close();
    }
  }

  @Test
  @DisplayName("a cursor closed by the server is signalled")
  void serverClosedCursorIsSignalled() throws Exception {
    MongoChangeStreamCursor<BsonDocument> cursor = openCursor();
    when(cursor.getServerCursor()).thenReturn(null);
    try (ChangeStreamPrefetcher prefetcher = start(cursor, 1, 10)) {
      ChangeStreamPrefetcher.FetchedBatch signal = prefetcher.poll(WAIT_MS);
      assertNotNull(signal);
      assertTrue(signal.isCursorClosedByServer());
      assertTrue(signal.getDocuments().isEmpty());
    }
  }

  @Test
  @DisplayName(
      "events buffered on a cursor the server already closed are delivered before the close")
  void bufferedEventsOnAClosedCursorAreDelivered() throws Exception {
    MongoChangeStreamCursor<BsonDocument> cursor = openCursor();
    when(cursor.getServerCursor()).thenReturn(null);
    when(cursor.available()).thenReturn(2, 1, 0);
    when(cursor.tryNext()).thenReturn(doc(1), doc(2));
    try (ChangeStreamPrefetcher prefetcher = start(cursor, 2, 10)) {
      ChangeStreamPrefetcher.FetchedBatch batch = prefetcher.poll(WAIT_MS);
      assertNotNull(batch);
      assertFalse(batch.isCursorClosedByServer());
      assertEquals(2, batch.getDocuments().size());
      assertEquals(doc(1), batch.getDocuments().get(0));
      assertEquals(doc(2), batch.getDocuments().get(1));

      ChangeStreamPrefetcher.FetchedBatch closed = prefetcher.poll(WAIT_MS);
      assertNotNull(closed);
      assertTrue(closed.isCursorClosedByServer());
    }
  }

  @Test
  @DisplayName("stop does not wait for a blocked getMore; the fetcher closes the cursor afterwards")
  void stopDoesNotWaitForBlockedGetMore() throws Exception {
    MongoChangeStreamCursor<BsonDocument> cursor = openCursor();
    CountDownLatch inGetMore = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    when(cursor.tryNext())
        .thenAnswer(
            i -> {
              inGetMore.countDown();
              release.await();
              return null;
            });
    ChangeStreamPrefetcher prefetcher = start(cursor, 1, 10);
    assertTrue(inGetMore.await(WAIT_MS, TimeUnit.MILLISECONDS));

    long start = System.nanoTime();
    prefetcher.stop();
    long stopMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    assertTrue(stopMs < 1000, "stop took " + stopMs + " ms");
    assertFalse(
        mockingDetailsClosed(cursor), "the cursor must stay open until the getMore returns");

    release.countDown();
    verify(cursor, timeout(WAIT_MS)).close();
  }

  @Test
  @DisplayName("an Error in the fetcher is reported to the task as a failure")
  void errorIsReported() throws Exception {
    MongoChangeStreamCursor<BsonDocument> cursor = openCursor();
    OutOfMemoryError error = new OutOfMemoryError("simulated");
    when(cursor.tryNext()).thenThrow(error);
    try (ChangeStreamPrefetcher prefetcher = start(cursor, 1, 10)) {
      ChangeStreamPrefetcher.FetchedBatch failed = prefetcher.poll(WAIT_MS);
      assertNotNull(failed);
      assertNotNull(failed.getError());
      assertSame(error, failed.getError().getCause());
    }
  }

  @Test
  @DisplayName("a fetcher that dies without reporting makes poll fail rather than return nothing")
  void silentFetcherExitFailsPoll() {
    MongoChangeStreamCursor<BsonDocument> cursor = openCursor();
    // an interrupted fetcher cannot queue its batch, and exits with nothing to report
    when(cursor.tryNext())
        .thenAnswer(
            i -> {
              Thread.currentThread().interrupt();
              return null;
            });
    try (ChangeStreamPrefetcher prefetcher = start(cursor, 1, 10)) {
      assertThrows(
          IllegalStateException.class,
          () -> {
            long deadline = System.currentTimeMillis() + WAIT_MS;
            while (System.currentTimeMillis() < deadline) {
              prefetcher.poll(50);
            }
          });
    }
  }

  @Test
  @DisplayName("time blocked on a full queue is reported")
  void queueFullWaitIsReported() throws Exception {
    MongoChangeStreamCursor<BsonDocument> cursor = openCursor();
    when(cursor.tryNext()).thenReturn(null);
    java.util.concurrent.atomic.AtomicLong waited = new java.util.concurrent.atomic.AtomicLong();
    try (ChangeStreamPrefetcher prefetcher =
        new ChangeStreamPrefetcher(cursor, 1, 10, 100, waited::addAndGet, "test-prefetcher")) {
      // the first batch fills the queue; the next offer blocks until it is taken
      Thread.sleep(300);
      assertNotNull(prefetcher.poll(WAIT_MS));
      assertNotNull(prefetcher.poll(WAIT_MS));
      // reported when the wait ends, just after the batch reaches the queue
      long deadline = System.currentTimeMillis() + WAIT_MS;
      while (waited.get() < 200 && System.currentTimeMillis() < deadline) {
        Thread.sleep(10);
      }
      assertTrue(waited.get() >= 200, "reported " + waited.get() + " ms");
    }
  }

  private static boolean mockingDetailsClosed(final MongoChangeStreamCursor<BsonDocument> cursor) {
    return org.mockito.Mockito.mockingDetails(cursor).getInvocations().stream()
        .anyMatch(i -> i.getMethod().getName().equals("close"));
  }
}
