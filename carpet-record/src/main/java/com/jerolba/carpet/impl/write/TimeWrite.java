/**
 * Copyright 2023 Jerónimo López Bezanilla
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
package com.jerolba.carpet.impl.write;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.function.BiConsumer;

import org.apache.parquet.io.api.RecordConsumer;

import com.jerolba.carpet.TimeUnit;

class TimeWrite {

    public static BiConsumer<RecordConsumer, Object> localDateTimeConsumer(TimeUnit timeUnit) {
        return switch (timeUnit) {
        case MILLIS -> (consumer, v) -> consumer.addLong(millisFromEpochFromLocalDateTime((LocalDateTime) v));
        case MICROS -> (consumer, v) -> consumer.addLong(microsFromEpochFromLocalDateTime((LocalDateTime) v));
        case NANOS -> (consumer, v) -> consumer.addLong(nanosFromEpochFromLocalDateTime((LocalDateTime) v));
        };
    }

    public static BiConsumer<RecordConsumer, Object> instantCosumer(TimeUnit timeUnit) {
        return switch (timeUnit) {
        case MILLIS -> (consumer, v) -> consumer.addLong(millisFromEpochFromInstant((Instant) v));
        case MICROS -> (consumer, v) -> consumer.addLong(microsFromEpochFromInstant((Instant) v));
        case NANOS -> (consumer, v) -> consumer.addLong(nanosFromEpochFromInstant((Instant) v));
        };
    }

    public static BiConsumer<RecordConsumer, Object> localTimeConsumer(TimeUnit timeUnit) {
        return switch (timeUnit) {
        case MILLIS -> (consumer, v) -> consumer.addInteger((int) (nanoTime(v) / 1_000_000L));
        case MICROS -> (consumer, v) -> consumer.addLong(nanoTime(v) / 1_000L);
        case NANOS -> (consumer, v) -> consumer.addLong(nanoTime(v));
        };
    }

    private static long nanoTime(Object v) {
        return ((LocalTime) v).toNanoOfDay();
    }

    private static long millisFromEpochFromLocalDateTime(LocalDateTime localDateTime) {
        Instant instant = timestampInUTCOffset(localDateTime);
        return millisFromEpochFromInstant(instant);
    }

    private static long microsFromEpochFromLocalDateTime(LocalDateTime localDateTime) {
        Instant instant = timestampInUTCOffset(localDateTime);
        return microsFromEpochFromInstant(instant);
    }

    private static long nanosFromEpochFromLocalDateTime(LocalDateTime localDateTime) {
        Instant instant = timestampInUTCOffset(localDateTime);
        return nanosFromEpochFromInstant(instant);
    }

    private static Instant timestampInUTCOffset(LocalDateTime timestamp) {
        return timestamp.toInstant(ZoneOffset.UTC);
    }

    private static long millisFromEpochFromInstant(Instant instant) {
        return instant.toEpochMilli();
    }

    private static long microsFromEpochFromInstant(Instant instant) {
        long seconds = instant.getEpochSecond();
        int nanos = instant.getNano();

        // Same implementation than Instant.toEpochMilli, but with 1_000_000L
        if (seconds < 0 && nanos > 0) {
            long micros = Math.multiplyExact(seconds + 1, 1_000_000L);
            long adjustment = (nanos / 1_000L) - 1_000_000L;
            return Math.addExact(micros, adjustment);
        } else {
            long micros = Math.multiplyExact(seconds, 1_000_000L);
            return Math.addExact(micros, nanos / 1_000L);
        }
    }

    private static long nanosFromEpochFromInstant(Instant instant) {
        long seconds = instant.getEpochSecond();
        int nanos = instant.getNano();

        // Same implementation than Instant.toEpochMilli, but with 1_000_000_000L
        if (seconds < 0 && nanos > 0) {
            long micros = Math.multiplyExact(seconds + 1, 1_000_000_000L);
            long adjustment = nanos - 1_000_000_000L;
            return Math.addExact(micros, adjustment);
        } else {
            long micros = Math.multiplyExact(seconds, 1_000_000_000L);
            return Math.addExact(micros, nanos);
        }
    }

}
