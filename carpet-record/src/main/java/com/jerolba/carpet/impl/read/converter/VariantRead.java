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
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.variant.Variant;
import org.apache.parquet.variant.Variant.ObjectField;
import org.apache.parquet.variant.Variant.Type;

public class VariantRead {

    private final LocalDateTimeRead localDateTimeRead = new LocalDateTimeRead();

    public Object deserialize(Variant variant) {
        if (variant == null) {
            return null;
        }
        Type type = variant.getType();
        return switch (type) {
        case NULL -> null;
        case BOOLEAN -> variant.getBoolean();
        case BYTE -> variant.getByte();
        case SHORT -> variant.getShort();
        case INT -> variant.getInt();
        case LONG -> variant.getLong();
        case FLOAT -> variant.getFloat();
        case DOUBLE -> variant.getDouble();
        case STRING -> variant.getString();
        case DECIMAL4, DECIMAL8, DECIMAL16 -> variant.getDecimal();
        case UUID -> variant.getUUID();
        case BINARY -> Binary.fromConstantByteBuffer(variant.getBinary());
        case ARRAY -> deserializeArray(variant);
        case OBJECT -> deserializeObject(variant);
        case DATE -> LocalDate.ofEpochDay(variant.getLong());
        case TIME -> LocalTime.ofNanoOfDay(variant.getLong() * 1000L);
        case TIMESTAMP_TZ -> InstantRead.instantFromMicrosFromEpoch(variant.getLong());
        case TIMESTAMP_NANOS_TZ -> InstantRead.instantFromNanosFromEpoch(variant.getLong());
        case TIMESTAMP_NTZ -> localDateTimeRead.localDateTimeFromMicrosFromEpoch(variant.getLong() * 1000L);
        case TIMESTAMP_NANOS_NTZ -> localDateTimeRead.localDateTimeFromNanosFromEpoch(variant.getLong());
        default -> throw new IllegalArgumentException("Unsupported variant type: " + type);
        };
    }

    private Map<String, Object> deserializeObject(Variant variant) {
        Map<String, Object> map = new HashMap<>();
        int size = variant.numObjectElements();
        for (int i = 0; i < size; i++) {
            ObjectField fieldAtIndex = variant.getFieldAtIndex(i);
            Object value = deserialize(fieldAtIndex.value);
            map.put(fieldAtIndex.key, value);
        }
        return map;
    }

    private List<Object> deserializeArray(Variant variant) {
        List<Object> list = new ArrayList<>();
        int size = variant.numArrayElements();
        for (int i = 0; i < size; i++) {
            Variant element = variant.getElementAtIndex(i);
            list.add(deserialize(element));
        }
        return list;
    }

}