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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.zone.ZoneRules;

public class LocalDateTimeRead {

    @FunctionalInterface
    public interface LongToLocalDateTime {
        LocalDateTime map(long timeFromEpoch);
    }

    private final ZoneRules rules = ZoneOffset.UTC.getRules();

    public LocalDateTime localDateTimeFromMillisFromEpoch(long millisFromEpoch) {
        Instant instant = InstantRead.instantFromMillisFromEpoch(millisFromEpoch);
        return localDateTimeInUTC(instant);
    }

    public LocalDateTime localDateTimeFromMicrosFromEpoch(long microsFromEpoch) {
        Instant instant = InstantRead.instantFromMicrosFromEpoch(microsFromEpoch);
        return localDateTimeInUTC(instant);
    }

    public LocalDateTime localDateTimeFromNanosFromEpoch(long nanosFromEpoch) {
        Instant instant = InstantRead.instantFromNanosFromEpoch(nanosFromEpoch);
        return localDateTimeInUTC(instant);
    }

    private LocalDateTime localDateTimeInUTC(Instant instant) {
        ZoneOffset offset = rules.getOffset(instant);
        return LocalDateTime.ofEpochSecond(instant.getEpochSecond(), instant.getNano(), offset);
    }

}
