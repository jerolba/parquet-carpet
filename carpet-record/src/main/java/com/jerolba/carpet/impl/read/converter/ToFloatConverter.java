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

import org.apache.parquet.io.api.PrimitiveConverter;

public class ToFloatConverter extends PrimitiveConverter {

    private final Consumer<Object> consumer;

    public ToFloatConverter(Consumer<Object> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void addDouble(double value) {
        consumer.accept((float) value);
    }

    @Override
    public void addFloat(float value) {
        consumer.accept(value);
    }

    @Override
    public void addLong(long value) {
        consumer.accept((float) value);
    }

    @Override
    public void addInt(int value) {
        consumer.accept((float) value);
    }

}
