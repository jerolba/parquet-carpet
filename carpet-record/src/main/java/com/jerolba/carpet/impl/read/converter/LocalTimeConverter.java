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

import java.time.LocalTime;
import java.util.function.Consumer;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;

public class LocalTimeConverter extends PrimitiveConverter {

    private final Consumer<Object> consumer;
    private LocalTime[] dict = null;
    private final long factor;
    private final TimeUnit timeUnit;

    public LocalTimeConverter(Consumer<Object> consumer, TimeUnit timeUnit) {
        this.consumer = consumer;
        this.timeUnit = timeUnit;
        this.factor = switch (timeUnit) {
        case MILLIS -> 1_000_000L;
        case MICROS -> 1000L;
        case NANOS -> 1L;
        };
    }

    @Override
    public void addInt(int millisInDay) {
        consumer.accept(convert(millisInDay));
    }

    @Override
    public void addLong(long microsOrNanos) {
        consumer.accept(convert(microsOrNanos));
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
        dict = new LocalTime[maxId + 1];
        if (timeUnit == TimeUnit.MILLIS) {
            for (int i = 0; i <= maxId; i++) {
                dict[i] = convert(dictionary.decodeToInt(i));
            }
        } else {
            for (int i = 0; i <= maxId; i++) {
                dict[i] = convert(dictionary.decodeToLong(i));
            }
        }
    }

    private LocalTime convert(long time) {
        return LocalTime.ofNanoOfDay(time * factor);
    }

}
