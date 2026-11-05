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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;

import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.lang.Nullable;

import com.mongodb.kafka.connect.util.time.MillisAccumulator;

/**
 * Reads change stream batches on a dedicated thread, so the next getMore runs on the server while
 * the task converts the previous batch and hands it to Kafka Connect.
 *
 * <p>The fetcher thread is the only thread that touches the cursor once the prefetcher is started.
 * Batches are queued in cursor order, each with the cursor's resume token as of the end of that
 * batch, so a consumer that has returned every document of a batch may safely use that batch's
 * token as an offset (e.g. for heartbeats). A cursor error or server-side cursor close is queued
 * after the batches read before it, and ends the fetcher.
 */
final class ChangeStreamPrefetcher implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeStreamPrefetcher.class);
  private static final long OFFER_RETRY_MS = 100;

  static final class FetchedBatch {
    private final List<BsonDocument> documents;
    @Nullable private final BsonDocument resumeToken;
    @Nullable private final RuntimeException error;
    private final boolean cursorClosedByServer;

    private FetchedBatch(
        final List<BsonDocument> documents,
        @Nullable final BsonDocument resumeToken,
        @Nullable final RuntimeException error,
        final boolean cursorClosedByServer) {
      this.documents = documents;
      this.resumeToken = resumeToken;
      this.error = error;
      this.cursorClosedByServer = cursorClosedByServer;
    }

    List<BsonDocument> getDocuments() {
      return documents;
    }

    /** The cursor resume token after the last document of this batch. */
    @Nullable
    BsonDocument getResumeToken() {
      return resumeToken;
    }

    @Nullable
    RuntimeException getError() {
      return error;
    }

    boolean isCursorClosedByServer() {
      return cursorClosedByServer;
    }
  }

  private final MongoChangeStreamCursor<? extends BsonDocument> cursor;
  private final BlockingQueue<FetchedBatch> queue;
  private final int maxBatchSize;
  private final long joinTimeoutMs;
  private final MillisAccumulator queueFullWait;
  private final Thread thread;
  private volatile boolean running = true;

  ChangeStreamPrefetcher(
      final MongoChangeStreamCursor<? extends BsonDocument> cursor,
      final int capacity,
      final int maxBatchSize,
      final long pollAwaitTimeMs,
      final LongConsumer queueFullWaitMillisConsumer,
      final String threadName) {
    this.queueFullWait = new MillisAccumulator(queueFullWaitMillisConsumer);
    this.cursor = cursor;
    this.queue = new ArrayBlockingQueue<>(capacity);
    this.maxBatchSize = maxBatchSize;
    // a getMore can block for up to maxAwaitTime on the server
    this.joinTimeoutMs = pollAwaitTimeMs + 10_000;
    this.thread = new Thread(this::run, threadName);
    thread.setDaemon(true);
    thread.start();
    LOGGER.info("Started change stream prefetcher {} (capacity {} batches)", threadName, capacity);
  }

  /**
   * @return the next batch in cursor order, or {@code null} if none arrived within the timeout
   * @throws IllegalStateException if the fetcher thread died without reporting a failure, so that
   *     the task fails rather than appearing healthy while delivering nothing
   */
  @Nullable
  FetchedBatch poll(final long timeoutMs) throws InterruptedException {
    FetchedBatch batch = queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
    if (batch == null && running && !thread.isAlive()) {
      // it may have queued its last batch just before exiting
      batch = queue.poll();
      if (batch == null) {
        throw new IllegalStateException(
            "Change stream prefetcher " + thread.getName() + " exited without reporting a result");
      }
    }
    return batch;
  }

  private void run() {
    try {
      while (running) {
        if (cursor.getServerCursor() == null) {
          enqueue(new FetchedBatch(Collections.emptyList(), null, null, true));
          return;
        }
        List<BsonDocument> batch = new ArrayList<>();
        BsonDocument next;
        do {
          next = cursor.tryNext();
          if (next != null) {
            batch.add(next);
          }
        } while (next != null && batch.size() < maxBatchSize && cursor.available() > 0);
        if (!enqueue(new FetchedBatch(batch, cursor.getResumeToken(), null, false))) {
          return;
        }
      }
    } catch (Throwable t) {
      if (running) {
        RuntimeException failure =
            t instanceof RuntimeException
                ? (RuntimeException) t
                : new RuntimeException("Change stream prefetcher failed: " + t, t);
        enqueue(new FetchedBatch(Collections.emptyList(), null, failure, false));
      } else {
        LOGGER.debug("Change stream prefetcher stopped with an exception during shutdown", t);
      }
    } finally {
      try {
        cursor.close();
      } catch (RuntimeException e) {
        if (running) {
          LOGGER.warn("Failed to close the change stream cursor in the prefetcher", e);
        } else {
          // task shutdown: the MongoClient may already be closed
          LOGGER.debug("Failed to close the change stream cursor during shutdown", e);
        }
      }
    }
  }

  /** @return false if the prefetcher was stopped before the batch could be queued */
  private boolean enqueue(final FetchedBatch batch) {
    long start = System.nanoTime();
    try {
      while (running) {
        if (queue.offer(batch, OFFER_RETRY_MS, TimeUnit.MILLISECONDS)) {
          return true;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      queueFullWait.add(System.nanoTime() - start);
    }
    return false;
  }

  /**
   * Signals the fetcher to stop, without waiting. The fetcher may be blocked in a getMore for up to
   * the poll await time; it closes the cursor when that returns. Use this on task shutdown, where
   * Kafka Connect limits how long a task may take to stop.
   */
  void stop() {
    running = false;
  }

  /**
   * Stops the fetcher thread and waits for it to exit; the fetcher closes the cursor on exit.
   * Queued batches are dropped.
   */
  @Override
  public void close() {
    running = false;
    try {
      thread.join(joinTimeoutMs);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    if (thread.isAlive()) {
      LOGGER.warn(
          "Change stream prefetcher {} did not stop within {} ms", thread.getName(), joinTimeoutMs);
    }
    queue.clear();
  }
}
