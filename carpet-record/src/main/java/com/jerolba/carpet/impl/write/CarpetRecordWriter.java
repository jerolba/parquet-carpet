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

import static com.jerolba.carpet.impl.AliasField.getFieldName;
import static com.jerolba.carpet.impl.Parameterized.getParameterizedCollection;
import static com.jerolba.carpet.impl.Parameterized.getParameterizedMap;

import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.parquet.io.api.RecordConsumer;

import com.jerolba.carpet.impl.JavaType;
import com.jerolba.carpet.impl.ParameterizedCollection;
import com.jerolba.carpet.impl.ParameterizedMap;
import com.jerolba.carpet.impl.write.FieldsWriter.BigDecimalFieldWriter;
import com.jerolba.carpet.impl.write.FieldsWriter.BooleanFieldWriter;
import com.jerolba.carpet.impl.write.FieldsWriter.DoubleFieldWriter;
import com.jerolba.carpet.impl.write.FieldsWriter.EnumFieldWriter;
import com.jerolba.carpet.impl.write.FieldsWriter.FloatFieldWriter;
import com.jerolba.carpet.impl.write.FieldsWriter.InstantFieldWriter;
import com.jerolba.carpet.impl.write.FieldsWriter.IntegerCompatibleFieldWriter;
import com.jerolba.carpet.impl.write.FieldsWriter.IntegerFieldWriter;
import com.jerolba.carpet.impl.write.FieldsWriter.LocalDateFieldWriter;
import com.jerolba.carpet.impl.write.FieldsWriter.LocalDateTimeFieldWriter;
import com.jerolba.carpet.impl.write.FieldsWriter.LocalTimeIntFieldWriter;
import com.jerolba.carpet.impl.write.FieldsWriter.LocalTimeLongFieldWriter;
import com.jerolba.carpet.impl.write.FieldsWriter.LongFieldWriter;
import com.jerolba.carpet.impl.write.FieldsWriter.StringFieldWriter;
import com.jerolba.carpet.impl.write.FieldsWriter.UuidFieldWriter;

public class CarpetRecordWriter {

    private final RecordConsumer recordConsumer;
    private final CarpetWriteConfiguration carpetConfiguration;

    private final List<Consumer<Object>> fieldWriters = new ArrayList<>();

    public CarpetRecordWriter(RecordConsumer recordConsumer, Class<?> recordClass,
            CarpetWriteConfiguration carpetConfiguration) {
        this.recordConsumer = recordConsumer;
        this.carpetConfiguration = carpetConfiguration;

        // Preconditions: All fields are writable
        int idx = 0;
        for (RecordComponent attr : recordClass.getRecordComponents()) {
            String fieldName = getFieldName(attr);

            Class<?> type = attr.getType();
            Consumer<Object> writer = null;
            RecordField f = new ReflectionRecordField(recordClass, attr, fieldName, idx);

            writer = buildBasicTypeWriter(type, f);

            if (writer == null) {
                writer = buildDateTypeWriter(type, f);
            }
            if (writer == null) {
                if (type.isRecord()) {
                    var recordWriter = new CarpetRecordWriter(recordConsumer, type, carpetConfiguration);
                    writer = new RecordFieldWriter(recordConsumer, f, recordWriter);
                } else if (Collection.class.isAssignableFrom(type)) {
                    ParameterizedCollection collectionClass = getParameterizedCollection(attr);
                    writer = createCollectionWriter(collectionClass, f);
                } else if (Map.class.isAssignableFrom(type)) {
                    ParameterizedMap mapClass = getParameterizedMap(attr);
                    writer = createMapWriter(mapClass, f);
                } else {
                    throw new RuntimeException(type.getName() + " can not be serialized");
                }
            }
            fieldWriters.add(writer);
            idx++;
        }
    }

    private Consumer<Object> createCollectionWriter(ParameterizedCollection collectionClass, RecordField f) {
        return switch (carpetConfiguration.annotatedLevels()) {
        case ONE -> new OneLevelStructureWriter(recordConsumer, carpetConfiguration)
                .createCollectionWriter(collectionClass, f);
        case TWO -> new TwoLevelStructureWriter(recordConsumer, carpetConfiguration)
                .createCollectionWriter(collectionClass, f);
        case THREE -> new ThreeLevelStructureWriter(recordConsumer, carpetConfiguration)
                .createCollectionWriter(collectionClass, f);
        };
    }

    private Consumer<Object> createMapWriter(ParameterizedMap mapClass, RecordField f) {
        return new MapStructureWriter(recordConsumer, carpetConfiguration).createMapWriter(mapClass, f);

    }

    private Consumer<Object> buildBasicTypeWriter(Class<?> javaType, RecordField f) {
        JavaType type = new JavaType(javaType);
        if (type.isInteger()) {
            return new IntegerFieldWriter(recordConsumer, f);
        } else if (type.isString()) {
            return new StringFieldWriter(recordConsumer, f);
        } else if (type.isBoolean()) {
            return new BooleanFieldWriter(recordConsumer, f);
        } else if (type.isLong()) {
            return new LongFieldWriter(recordConsumer, f);
        } else if (type.isDouble()) {
            return new DoubleFieldWriter(recordConsumer, f);
        } else if (type.isFloat()) {
            return new FloatFieldWriter(recordConsumer, f);
        } else if (type.isShort() || type.isByte()) {
            return new IntegerCompatibleFieldWriter(recordConsumer, f);
        } else if (type.isEnum()) {
            return new EnumFieldWriter(recordConsumer, f, type.getJavaType());
        } else if (type.isUuid()) {
            return new UuidFieldWriter(recordConsumer, f);
        } else if (type.isBigDecimal()) {
            return new BigDecimalFieldWriter(recordConsumer, f, carpetConfiguration.decimalConfig());
        }
        return null;
    }

    private Consumer<Object> buildDateTypeWriter(Class<?> javaType, RecordField f) {
        JavaType type = new JavaType(javaType);
        if (type.isLocalDate()) {
            return new LocalDateFieldWriter(recordConsumer, f);
        }
        if (type.isLocalTime()) {
            return switch (carpetConfiguration.defaultTimeUnit()) {
            case MILLIS -> new LocalTimeIntFieldWriter(recordConsumer, f);
            case MICROS -> new LocalTimeLongFieldWriter(recordConsumer, f, 1_000);
            case NANOS -> new LocalTimeLongFieldWriter(recordConsumer, f, 1);
            };
        }
        if (type.isLocalDateTime()) {
            return new LocalDateTimeFieldWriter(recordConsumer, f, carpetConfiguration.defaultTimeUnit());
        }
        if (type.isInstant()) {
            return new InstantFieldWriter(recordConsumer, f, carpetConfiguration.defaultTimeUnit());
        }
        return null;
    }

    public void write(Object record) {
        for (var fieldWriter : fieldWriters) {
            fieldWriter.accept(record);
        }
    }

    private static class RecordFieldWriter implements Consumer<Object> {

        private final RecordConsumer recordConsumer;
        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;
        private final CarpetRecordWriter writer;

        RecordFieldWriter(RecordConsumer recordConsumer, RecordField recordField, CarpetRecordWriter writer) {
            this.recordConsumer = recordConsumer;
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = recordField.getAccessor();
            this.writer = writer;
        }

        @Override
        public void accept(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(fieldName, idx);
                recordConsumer.startGroup();
                writer.write(value);
                recordConsumer.endGroup();
                recordConsumer.endField(fieldName, idx);
            }
        }
    }

}
