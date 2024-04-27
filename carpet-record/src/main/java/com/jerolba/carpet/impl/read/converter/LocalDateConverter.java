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

import java.time.LocalDate;
import java.util.function.Consumer;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.PrimitiveConverter;

public class LocalDateConverter extends PrimitiveConverter {

    private LocalDate[] dict = null;
    private final Consumer<Object> listConsumer;

    public LocalDateConverter(Consumer<Object> listConsumer) {
        this.listConsumer = listConsumer;
    }

    @Override
    public void addInt(int daysFromEpoch) {
        listConsumer.accept(LocalDate.ofEpochDay(daysFromEpoch));
    }

    @Override
    public boolean hasDictionarySupport() {
        return true;
    }

    @Override
    public void setDictionary(Dictionary dictionary) {
        int maxId = dictionary.getMaxId();
        dict = new LocalDate[maxId + 1];
        for (int i = 0; i <= maxId; i++) {
            dict[i] = convert(dictionary.decodeToInt(i));
        }
    }

    @Override
    public void addValueFromDictionary(int dictionaryId) {
        listConsumer.accept(dict[dictionaryId]);
    }

    private LocalDate convert(int daysFromEpoch) {
        return LocalDate.ofEpochDay(daysFromEpoch);
    }

}
