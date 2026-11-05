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
package com.mongodb.kafka.connect.util.time;

import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;

import com.mongodb.annotations.NotThreadSafe;

/**
 * Reports durations in whole milliseconds, carrying the remainder forward so that many
 * sub-millisecond durations are not lost to truncation.
 */
@NotThreadSafe
public final class MillisAccumulator {
  private static final long NANOS_PER_MILLI = TimeUnit.MILLISECONDS.toNanos(1);

  private final LongConsumer millisConsumer;
  private long remainderNanos;

  public MillisAccumulator(final LongConsumer millisConsumer) {
    this.millisConsumer = millisConsumer;
  }

  public void add(final long nanos) {
    remainderNanos += nanos;
    long millis = remainderNanos / NANOS_PER_MILLI;
    if (millis > 0) {
      remainderNanos -= millis * NANOS_PER_MILLI;
      millisConsumer.accept(millis);
    }
  }
}
