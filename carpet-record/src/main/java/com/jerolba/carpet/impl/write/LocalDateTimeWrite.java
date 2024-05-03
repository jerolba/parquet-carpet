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
import java.time.ZoneOffset;

class LocalDateTimeWrite {

    @FunctionalInterface
    public interface LocalDateTimeToLong {
        long map(LocalDateTime localdateTime);
    }

    public static long millisFromEpochFromLocalDateTime(LocalDateTime localDateTime) {
        Instant instant = timestampInUTCOffset(localDateTime);
        return InstantWrite.millisFromEpochFromInstant(instant);
    }

    public static long microsFromEpochFromLocalDateTime(LocalDateTime localDateTime) {
        Instant instant = timestampInUTCOffset(localDateTime);
        return InstantWrite.microsFromEpochFromInstant(instant);
    }

    public static long nanosFromEpochFromLocalDateTime(LocalDateTime localDateTime) {
        Instant instant = timestampInUTCOffset(localDateTime);
        return InstantWrite.nanosFromEpochFromInstant(instant);
    }

    private static Instant timestampInUTCOffset(LocalDateTime timestamp) {
        return timestamp.toInstant(ZoneOffset.UTC);
    }

}
