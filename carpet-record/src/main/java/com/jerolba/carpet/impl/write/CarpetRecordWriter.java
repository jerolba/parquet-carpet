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
import static com.jerolba.carpet.impl.write.UuidWrite.uuidToBinary;

import java.lang.reflect.RecordComponent;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

import com.jerolba.carpet.TimeUnit;
import com.jerolba.carpet.impl.JavaType;
import com.jerolba.carpet.impl.ParameterizedCollection;
import com.jerolba.carpet.impl.ParameterizedMap;
import com.jerolba.carpet.impl.write.InstantWrite.InstantToLong;
import com.jerolba.carpet.impl.write.LocalDateTimeWrite.LocalDateTimeToLong;

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
            RecordField f = new RecordField(recordClass, attr, fieldName, idx);

            writer = buildBasicTypeWriter(type, f);

            if (writer == null) {
                writer = buildDateTypeWriter(type, f);
            }
            if (writer == null) {
                if (type.isRecord()) {
                    var recordWriter = new CarpetRecordWriter(recordConsumer, type, carpetConfiguration);
                    writer = new RecordFieldWriter(f, recordWriter);
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
            return new IntegerFieldWriter(f);
        } else if (type.isString()) {
            return new StringFieldWriter(f);
        } else if (type.isBoolean()) {
            return new BooleanFieldWriter(f);
        } else if (type.isLong()) {
            return new LongFieldWriter(f);
        } else if (type.isDouble()) {
            return new DoubleFieldWriter(f);
        } else if (type.isFloat()) {
            return new FloatFieldWriter(f);
        } else if (type.isShort() || type.isByte()) {
            return new IntegerCompatibleFieldWriter(f);
        } else if (type.isEnum()) {
            return new EnumFieldWriter(f, type.getJavaType());
        } else if (type.isUuid()) {
            return new UuidFieldWriter(f);
        } else if (type.isBigDecimal()) {
            return new BigDecimalFieldWriter(f, recordConsumer, carpetConfiguration.decimalConfig());
        }
        return null;
    }

    private Consumer<Object> buildDateTypeWriter(Class<?> javaType, RecordField f) {
        JavaType type = new JavaType(javaType);
        if (type.isLocalDate()) {
            return new LocalDateFieldWriter(f, recordConsumer);
        }
        if (type.isLocalTime()) {
            return switch (carpetConfiguration.defaultTimeUnit()) {
            case MILLIS -> new LocalTimeIntFieldWriter(f, recordConsumer);
            case MICROS -> new LocalTimeLongFieldWriter(f, recordConsumer, 1_000);
            case NANOS -> new LocalTimeLongFieldWriter(f, recordConsumer, 1);
            };
        }
        if (type.isLocalDateTime()) {
            return new LocalDateTimeFieldWriter(f, recordConsumer, carpetConfiguration.defaultTimeUnit());
        }
        if (type.isInstant()) {
            return new InstantFieldWriter(f, recordConsumer, carpetConfiguration.defaultTimeUnit());
        }
        return null;
    }

    public void write(Object record) {
        for (var fieldWriter : fieldWriters) {
            fieldWriter.accept(record);
        }
    }

    private class IntegerFieldWriter implements Consumer<Object> {

        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;

        IntegerFieldWriter(RecordField recordField) {
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = Reflection.recordAccessor(recordField.targetClass(), recordField.recordComponent());
        }

        @Override
        public void accept(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(fieldName, idx);
                recordConsumer.addInteger((Integer) value);
                recordConsumer.endField(fieldName, idx);
            }
        }
    }

    private class IntegerCompatibleFieldWriter implements Consumer<Object> {

        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;

        IntegerCompatibleFieldWriter(RecordField recordField) {
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = Reflection.recordAccessor(recordField.targetClass(), recordField.recordComponent());
        }

        @Override
        public void accept(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(fieldName, idx);
                recordConsumer.addInteger(((Number) value).intValue());
                recordConsumer.endField(fieldName, idx);
            }
        }
    }

    private class LongFieldWriter implements Consumer<Object> {

        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;

        LongFieldWriter(RecordField recordField) {
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = Reflection.recordAccessor(recordField.targetClass(), recordField.recordComponent());
        }

        @Override
        public void accept(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(fieldName, idx);
                recordConsumer.addLong((Long) value);
                recordConsumer.endField(fieldName, idx);
            }
        }
    }

    private class BooleanFieldWriter implements Consumer<Object> {

        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;

        BooleanFieldWriter(RecordField recordField) {
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = Reflection.recordAccessor(recordField.targetClass(), recordField.recordComponent());
        }

        @Override
        public void accept(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(fieldName, idx);
                recordConsumer.addBoolean((Boolean) value);
                recordConsumer.endField(fieldName, idx);
            }
        }
    }

    private class FloatFieldWriter implements Consumer<Object> {

        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;

        FloatFieldWriter(RecordField recordField) {
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = Reflection.recordAccessor(recordField.targetClass(), recordField.recordComponent());
        }

        @Override
        public void accept(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(fieldName, idx);
                recordConsumer.addFloat((Float) value);
                recordConsumer.endField(fieldName, idx);
            }
        }
    }

    private class DoubleFieldWriter implements Consumer<Object> {

        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;

        DoubleFieldWriter(RecordField recordField) {
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = Reflection.recordAccessor(recordField.targetClass(), recordField.recordComponent());
        }

        @Override
        public void accept(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(fieldName, idx);
                recordConsumer.addDouble((Double) value);
                recordConsumer.endField(fieldName, idx);
            }
        }
    }

    private class StringFieldWriter implements Consumer<Object> {

        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;

        StringFieldWriter(RecordField recordField) {
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = Reflection.recordAccessor(recordField.targetClass(), recordField.recordComponent());
        }

        @Override
        public void accept(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(fieldName, idx);
                recordConsumer.addBinary(Binary.fromString((String) value));
                recordConsumer.endField(fieldName, idx);
            }
        }
    }

    private class EnumFieldWriter implements Consumer<Object> {

        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;
        private final EnumsValues values;

        EnumFieldWriter(RecordField recordField, Class<?> enumClass) {
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = Reflection.recordAccessor(recordField.targetClass(), recordField.recordComponent());
            this.values = new EnumsValues(enumClass);
        }

        @Override
        public void accept(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(fieldName, idx);
                recordConsumer.addBinary(values.getValue(value));
                recordConsumer.endField(fieldName, idx);
            }
        }

    }

    private class UuidFieldWriter implements Consumer<Object> {

        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;

        UuidFieldWriter(RecordField recordField) {
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = Reflection.recordAccessor(recordField.targetClass(), recordField.recordComponent());
        }

        @Override
        public void accept(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(fieldName, idx);
                recordConsumer.addBinary(uuidToBinary(value));
                recordConsumer.endField(fieldName, idx);
            }
        }

    }

    private static class LocalDateFieldWriter implements Consumer<Object> {

        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;
        private final RecordConsumer recordConsumer;

        LocalDateFieldWriter(RecordField recordField, RecordConsumer recordConsumer) {
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = Reflection.recordAccessor(recordField.targetClass(), recordField.recordComponent());
            this.recordConsumer = recordConsumer;
        }

        @Override
        public void accept(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                long epochDay = ((LocalDate) value).toEpochDay();
                recordConsumer.startField(fieldName, idx);
                recordConsumer.addInteger((int) epochDay);
                recordConsumer.endField(fieldName, idx);
            }
        }
    }

    private static class LocalTimeIntFieldWriter implements Consumer<Object> {

        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;
        private final RecordConsumer recordConsumer;

        LocalTimeIntFieldWriter(RecordField recordField, RecordConsumer recordConsumer) {
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = Reflection.recordAccessor(recordField.targetClass(), recordField.recordComponent());
            this.recordConsumer = recordConsumer;
        }

        @Override
        public void accept(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                long milliOfDay = ((LocalTime) value).toNanoOfDay() / 1_000_000;
                recordConsumer.startField(fieldName, idx);
                recordConsumer.addInteger((int) milliOfDay);
                recordConsumer.endField(fieldName, idx);
            }
        }
    }

    private static class LocalTimeLongFieldWriter implements Consumer<Object> {

        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;
        private final RecordConsumer recordConsumer;
        private final int factor;

        LocalTimeLongFieldWriter(RecordField recordField, RecordConsumer recordConsumer, int factor) {
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = Reflection.recordAccessor(recordField.targetClass(), recordField.recordComponent());
            this.recordConsumer = recordConsumer;
            this.factor = factor;
        }

        @Override
        public void accept(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                long offsetOfDay = ((LocalTime) value).toNanoOfDay() / factor;
                recordConsumer.startField(fieldName, idx);
                recordConsumer.addLong(offsetOfDay);
                recordConsumer.endField(fieldName, idx);
            }
        }
    }

    private static class LocalDateTimeFieldWriter implements Consumer<Object> {

        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;
        private final RecordConsumer recordConsumer;
        private final LocalDateTimeToLong mapper;

        LocalDateTimeFieldWriter(RecordField recordField, RecordConsumer recordConsumer, TimeUnit timeUnit) {
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = Reflection.recordAccessor(recordField.targetClass(), recordField.recordComponent());
            this.recordConsumer = recordConsumer;
            this.mapper = switch (timeUnit) {
            case MILLIS -> LocalDateTimeWrite::millisFromEpochFromLocalDateTime;
            case MICROS -> LocalDateTimeWrite::microsFromEpochFromLocalDateTime;
            case NANOS -> LocalDateTimeWrite::nanosFromEpochFromLocalDateTime;
            };
        }

        @Override
        public void accept(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(fieldName, idx);
                recordConsumer.addLong(mapper.map((LocalDateTime) value));
                recordConsumer.endField(fieldName, idx);
            }
        }
    }

    private static class InstantFieldWriter implements Consumer<Object> {

        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;
        private final RecordConsumer recordConsumer;
        private final InstantToLong mapper;

        InstantFieldWriter(RecordField recordField, RecordConsumer recordConsumer, TimeUnit timeUnit) {
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = Reflection.recordAccessor(recordField.targetClass(), recordField.recordComponent());
            this.recordConsumer = recordConsumer;
            this.mapper = switch (timeUnit) {
            case MILLIS -> InstantWrite::millisFromEpochFromInstant;
            case MICROS -> InstantWrite::microsFromEpochFromInstant;
            case NANOS -> InstantWrite::nanosFromEpochFromInstant;
            };
        }

        @Override
        public void accept(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(fieldName, idx);
                recordConsumer.addLong(mapper.map((Instant) value));
                recordConsumer.endField(fieldName, idx);
            }
        }
    }

    public static class BigDecimalFieldWriter implements Consumer<Object> {

        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;
        private final RecordConsumer recordConsumer;
        private final BigDecimalWrite bigDecimalWrite;

        BigDecimalFieldWriter(RecordField recordField, RecordConsumer recordConsumer, DecimalConfig decimalConfig) {
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = Reflection.recordAccessor(recordField.targetClass(), recordField.recordComponent());
            this.recordConsumer = recordConsumer;
            this.bigDecimalWrite = new BigDecimalWrite(decimalConfig);
        }

        @Override
        public void accept(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(fieldName, idx);
                bigDecimalWrite.write(recordConsumer, value);
                recordConsumer.endField(fieldName, idx);
            }
        }

    }

    private class RecordFieldWriter implements Consumer<Object> {

        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;
        private final CarpetRecordWriter writer;

        RecordFieldWriter(RecordField recordField, CarpetRecordWriter writer) {
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = Reflection.recordAccessor(recordField.targetClass(), recordField.recordComponent());
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
