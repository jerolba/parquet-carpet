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
package com.jerolba.carpet.impl.read.converter;

import java.time.Instant;

class InstantRead {

    @FunctionalInterface
    public interface LongToInstant {
        Instant map(long timeFromEpoch);
    }

    public static Instant instantFromMillisFromEpoch(long millisFromEpoch) {
        return Instant.ofEpochMilli(millisFromEpoch);
    }

    public static Instant instantFromMicrosFromEpoch(long microsFromEpoch) {
        long secs = Math.floorDiv(microsFromEpoch, 1_000_000L);
        long nanos = Math.floorMod(microsFromEpoch, 1_000_000L) * 1000L;
        return Instant.ofEpochSecond(secs, nanos);
    }

    public static Instant instantFromNanosFromEpoch(long nanosFromEpoch) {
        long secs = Math.floorDiv(nanosFromEpoch, 1_000_000_000L);
        long nanos = Math.floorMod(nanosFromEpoch, 1_000_000_000L);
        return Instant.ofEpochSecond(secs, nanos);
    }
}
