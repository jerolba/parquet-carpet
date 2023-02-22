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

import java.util.function.BiConsumer;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

import com.jerolba.carpet.CarpetWriteConfiguration;

public class SimpleCollectionItemConsumerFactory {

    public static BiConsumer<RecordConsumer, Object> buildSimpleElementConsumer(Class<?> type,
            RecordConsumer recordConsumer, CarpetWriteConfiguration carpetConfiguration) {

        BiConsumer<RecordConsumer, Object> elemConsumer = null;
        String typeName = type.getName();
        if (typeName.equals("int") || typeName.equals("java.lang.Integer")) {
            return (consumer, v) -> consumer.addInteger((Integer) v);
        }
        if (typeName.equals("java.lang.String")) {
            return (consumer, v) -> consumer.addBinary(Binary.fromString((String) v));
        }
        if (typeName.equals("boolean") || typeName.equals("java.lang.Boolean")) {
            return (consumer, v) -> consumer.addBoolean((Boolean) v);
        }
        if (typeName.equals("long") || typeName.equals("java.lang.Long")) {
            return (consumer, v) -> consumer.addLong((Long) v);
        }
        if (typeName.equals("double") || typeName.equals("java.lang.Double")) {
            return (consumer, v) -> consumer.addDouble((Double) v);
        }
        if (typeName.equals("float") || typeName.equals("java.lang.Float")) {
            return (consumer, v) -> consumer.addFloat((Float) v);
        }
        if (typeName.equals("short") || typeName.equals("java.lang.Short")
                || typeName.equals("byte") || typeName.equals("java.lang.Byte")) {
            return (consumer, v) -> consumer.addInteger(((Number) v).intValue());
        }
        if (type.isEnum()) {
            EnumsValues enumValues = new EnumsValues(type);
            return (consumer, v) -> consumer.addBinary(enumValues.getValue(v));
        }
        if (type.isRecord()) {
            CarpetRecordWriter recordWriter = new CarpetRecordWriter(recordConsumer, type, carpetConfiguration);
            return (consumer, v) -> {
                consumer.startGroup();
                recordWriter.write(v);
                consumer.endGroup();
            };
        }
        return elemConsumer;
    }
}
