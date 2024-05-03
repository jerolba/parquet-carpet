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

class InstantWrite {

    @FunctionalInterface
    public interface InstantToLong {
        long map(Instant instant);
    }

    public static long millisFromEpochFromInstant(Instant instant) {
        return instant.toEpochMilli();
    }

    public static long microsFromEpochFromInstant(Instant instant) {
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

    public static long nanosFromEpochFromInstant(Instant instant) {
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
