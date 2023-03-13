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

import static com.jerolba.carpet.impl.write.UuidWrite.uuidToBinary;

import java.util.function.BiConsumer;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

import com.jerolba.carpet.impl.JavaType;

public class SimpleCollectionItemConsumerFactory {

    public static BiConsumer<RecordConsumer, Object> buildSimpleElementConsumer(Class<?> javaType,
            RecordConsumer recordConsumer, CarpetWriteConfiguration carpetConfiguration) {

        BiConsumer<RecordConsumer, Object> elemConsumer = null;
        JavaType type = new JavaType(javaType);
        if (type.isInteger()) {
            return (consumer, v) -> consumer.addInteger((Integer) v);
        }
        if (type.isString()) {
            return (consumer, v) -> consumer.addBinary(Binary.fromString((String) v));
        }
        if (type.isBoolean()) {
            return (consumer, v) -> consumer.addBoolean((Boolean) v);
        }
        if (type.isLong()) {
            return (consumer, v) -> consumer.addLong((Long) v);
        }
        if (type.isDouble()) {
            return (consumer, v) -> consumer.addDouble((Double) v);
        }
        if (type.isFloat()) {
            return (consumer, v) -> consumer.addFloat((Float) v);
        }
        if (type.isShort() || type.isByte()) {
            return (consumer, v) -> consumer.addInteger(((Number) v).intValue());
        }
        if (type.isEnum()) {
            EnumsValues enumValues = new EnumsValues(type.getJavaType());
            return (consumer, v) -> consumer.addBinary(enumValues.getValue(v));
        }
        if (type.isUuid()) {
            return (consumer, v) -> consumer.addBinary(uuidToBinary(v));
        }
        if (type.isRecord()) {
            var recordWriter = new CarpetRecordWriter(recordConsumer, type.getJavaType(), carpetConfiguration);
            return (consumer, v) -> {
                consumer.startGroup();
                recordWriter.write(v);
                consumer.endGroup();
            };
        }
        return elemConsumer;
    }
}
