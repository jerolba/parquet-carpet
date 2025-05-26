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

import java.time.LocalDateTime;
import java.util.function.Consumer;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;

import com.jerolba.carpet.impl.read.converter.LocalDateTimeRead.LongToLocalDateTime;

public class LocalDateTimeConverter extends PrimitiveConverter {

    private final LocalDateTimeRead localDateTimeRead = new LocalDateTimeRead();
    private final Consumer<Object> consumer;
    private final LongToLocalDateTime mapper;
    private LocalDateTime[] dict = null;

    public LocalDateTimeConverter(Consumer<Object> consumer, TimeUnit timeUnit) {
        this.consumer = consumer;
        this.mapper = switch (timeUnit) {
        case MILLIS -> localDateTimeRead::localDateTimeFromMillisFromEpoch;
        case MICROS -> localDateTimeRead::localDateTimeFromMicrosFromEpoch;
        case NANOS -> localDateTimeRead::localDateTimeFromNanosFromEpoch;
        };
    }

    @Override
    public void addLong(long timeToEpoch) {
        consumer.accept(mapper.map(timeToEpoch));
    }

    @Override
    public void addValueFromDictionary(int dictionaryId) {
        consumer.accept(dict[dictionaryId]);
    }

    @Override
    public boolean hasDictionarySupport() {
        return true;
    }

    @Override
    public void setDictionary(Dictionary dictionary) {
        int maxId = dictionary.getMaxId();
        dict = new LocalDateTime[maxId + 1];
        for (int i = 0; i <= maxId; i++) {
            dict[i] = mapper.map(dictionary.decodeToLong(i));
        }
    }

}
