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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;

public class DecimalConverter extends PrimitiveConverter {

    private final Consumer<Object> consumer;
    private final int scale;
    private BigDecimal[] dict = null;

    public DecimalConverter(Consumer<Object> consumer, int scale) {
        this.consumer = consumer;
        this.scale = scale;
    }

    @Override
    public void addBinary(Binary value) {
        consumer.accept(convert(value));
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
        dict = new BigDecimal[maxId + 1];
        for (int i = 0; i <= maxId; i++) {
            dict[i] = convert(dictionary.decodeToBinary(i));
        }
    }

    // From org.apache.avro.Conversion$DecimalConversion
    private BigDecimal convert(Binary binary) {
        ByteBuffer value = ByteBuffer.wrap(binary.getBytes());
        // always copy the bytes out because BigInteger has no offset/length constructor
        byte[] bytes = new byte[value.remaining()];
        value.duplicate().get(bytes);
        return new BigDecimal(new BigInteger(bytes), scale);
    }

}
