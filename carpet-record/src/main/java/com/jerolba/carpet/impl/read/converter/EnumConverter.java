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

import java.util.function.Consumer;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;

public class EnumConverter extends PrimitiveConverter {

    private Enum<?>[] dict = null;
    private final Consumer<Object> listConsumer;
    private final Class<? extends Enum> asEnum;

    public EnumConverter(Consumer<Object> listConsumer, Class<?> type) {
        this.listConsumer = listConsumer;
        this.asEnum = type.asSubclass(Enum.class);
    }

    @Override
    public void addBinary(Binary value) {
        listConsumer.accept(convert(value));
    }

    @Override
    public boolean hasDictionarySupport() {
        return true;
    }

    @Override
    public void setDictionary(Dictionary dictionary) {
        int maxId = dictionary.getMaxId();
        dict = new Enum[maxId + 1];
        for (int i = 0; i <= maxId; i++) {
            dict[i] = convert(dictionary.decodeToBinary(i));
        }
    }

    @Override
    public void addValueFromDictionary(int dictionaryId) {
        listConsumer.accept(dict[dictionaryId]);
    }

    private Enum<?> convert(Binary value) {
        String str = value.toStringUsingUTF8();
        return Enum.valueOf(asEnum, str);
    }
}