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

import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;

import com.jerolba.carpet.impl.read.ReadReflection.ConstructorParams;

public class UuidToUuidConverter extends PrimitiveConverter {

    private UUID[] dict = null;
    private final ConstructorParams constructor;
    private final int idx;

    public UuidToUuidConverter(ConstructorParams constructor, int idx) {
        this.constructor = constructor;
        this.idx = idx;
    }

    @Override
    public void addBinary(Binary value) {
        constructor.c[idx] = convert(value);
    }

    @Override
    public boolean hasDictionarySupport() {
        return true;
    }

    @Override
    public void setDictionary(Dictionary dictionary) {
        dict = new UUID[dictionary.getMaxId() + 1];
        for (int i = 0; i <= dictionary.getMaxId(); i++) {
            dict[i] = convert(dictionary.decodeToBinary(i));
        }
    }

    @Override
    public void addValueFromDictionary(int dictionaryId) {
        constructor.c[idx] = dict[dictionaryId];
    }

    private UUID convert(Binary value) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(value.getBytes());
        long high = byteBuffer.getLong();
        long low = byteBuffer.getLong();
        return new UUID(high, low);
    }

}