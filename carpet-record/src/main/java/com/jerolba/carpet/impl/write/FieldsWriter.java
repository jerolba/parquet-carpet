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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

import com.jerolba.carpet.TimeUnit;
import com.jerolba.carpet.impl.write.InstantWrite.InstantToLong;
import com.jerolba.carpet.impl.write.LocalDateTimeWrite.LocalDateTimeToLong;

class FieldsWriter {

    private FieldsWriter() {
    }

    public static class IntegerFieldWriter implements Consumer<Object> {

        private final RecordConsumer recordConsumer;
        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;

        IntegerFieldWriter(RecordConsumer recordConsumer, RecordField recordField) {
            this.recordConsumer = recordConsumer;
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = recordField.getAccessor();
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

    public static class IntegerCompatibleFieldWriter implements Consumer<Object> {

        private final RecordConsumer recordConsumer;
        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;

        IntegerCompatibleFieldWriter(RecordConsumer recordConsumer, RecordField recordField) {
            this.recordConsumer = recordConsumer;
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = recordField.getAccessor();
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

    public static class LongFieldWriter implements Consumer<Object> {

        private final RecordConsumer recordConsumer;
        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;

        LongFieldWriter(RecordConsumer recordConsumer, RecordField recordField) {
            this.recordConsumer = recordConsumer;
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = recordField.getAccessor();
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

    public static class BooleanFieldWriter implements Consumer<Object> {

        private final RecordConsumer recordConsumer;
        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;

        BooleanFieldWriter(RecordConsumer recordConsumer, RecordField recordField) {
            this.recordConsumer = recordConsumer;
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = recordField.getAccessor();
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

    public static class FloatFieldWriter implements Consumer<Object> {

        private final RecordConsumer recordConsumer;
        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;

        FloatFieldWriter(RecordConsumer recordConsumer, RecordField recordField) {
            this.recordConsumer = recordConsumer;
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = recordField.getAccessor();
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

    public static class DoubleFieldWriter implements Consumer<Object> {

        private final RecordConsumer recordConsumer;
        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;

        DoubleFieldWriter(RecordConsumer recordConsumer, RecordField recordField) {
            this.recordConsumer = recordConsumer;
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = recordField.getAccessor();
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

    public static class StringFieldWriter implements Consumer<Object> {

        private final RecordConsumer recordConsumer;
        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;

        StringFieldWriter(RecordConsumer recordConsumer, RecordField recordField) {
            this.recordConsumer = recordConsumer;
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = recordField.getAccessor();
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

    public static class EnumFieldWriter implements Consumer<Object> {

        private final RecordConsumer recordConsumer;
        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;
        private final EnumsValues values;

        EnumFieldWriter(RecordConsumer recordConsumer, RecordField recordField, Class<?> enumClass) {
            this.recordConsumer = recordConsumer;
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = recordField.getAccessor();
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

    public static class UuidFieldWriter implements Consumer<Object> {

        private final RecordConsumer recordConsumer;
        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;

        UuidFieldWriter(RecordConsumer recordConsumer, RecordField recordField) {
            this.recordConsumer = recordConsumer;
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = recordField.getAccessor();
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

    public static class LocalDateFieldWriter implements Consumer<Object> {

        private final RecordConsumer recordConsumer;
        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;

        LocalDateFieldWriter(RecordConsumer recordConsumer, RecordField recordField) {
            this.recordConsumer = recordConsumer;
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = recordField.getAccessor();
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

    public static class LocalTimeIntFieldWriter implements Consumer<Object> {

        private final RecordConsumer recordConsumer;
        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;

        LocalTimeIntFieldWriter(RecordConsumer recordConsumer, RecordField recordField) {
            this.recordConsumer = recordConsumer;
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = recordField.getAccessor();
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

    public static class LocalTimeLongFieldWriter implements Consumer<Object> {

        private final RecordConsumer recordConsumer;
        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;
        private final int factor;

        LocalTimeLongFieldWriter(RecordConsumer recordConsumer, RecordField recordField, int factor) {
            this.recordConsumer = recordConsumer;
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = recordField.getAccessor();
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

    public static class LocalDateTimeFieldWriter implements Consumer<Object> {

        private final RecordConsumer recordConsumer;
        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;
        private final LocalDateTimeToLong mapper;

        LocalDateTimeFieldWriter(RecordConsumer recordConsumer, RecordField recordField, TimeUnit timeUnit) {
            this.recordConsumer = recordConsumer;
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = recordField.getAccessor();
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

    public static class InstantFieldWriter implements Consumer<Object> {

        private final RecordConsumer recordConsumer;
        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;
        private final InstantToLong mapper;

        InstantFieldWriter(RecordConsumer recordConsumer, RecordField recordField, TimeUnit timeUnit) {
            this.recordConsumer = recordConsumer;
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = recordField.getAccessor();
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

        private final RecordConsumer recordConsumer;
        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;
        private final BigDecimalWrite bigDecimalWrite;

        BigDecimalFieldWriter(RecordConsumer recordConsumer, RecordField recordField, DecimalConfig decimalConfig) {
            this.recordConsumer = recordConsumer;
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = recordField.getAccessor();
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

    public static class RecordFieldWriter implements Consumer<Object> {

        private final RecordConsumer recordConsumer;
        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;
        private final Consumer<Object> writer;

        RecordFieldWriter(RecordConsumer recordConsumer, RecordField recordField, Consumer<Object> writer) {
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
                writer.accept(value);
                recordConsumer.endGroup();
                recordConsumer.endField(fieldName, idx);
            }
        }
    }

}
