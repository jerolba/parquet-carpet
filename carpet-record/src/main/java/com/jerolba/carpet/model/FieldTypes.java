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
package com.jerolba.carpet.model;

public class FieldTypes {

    public static final BooleanType BOOLEAN = new BooleanType(false);
    public static final ByteType BYTE = new ByteType(false);
    public static final ShortType SHORT = new ShortType(false);
    public static final IntegerType INTEGER = new IntegerType(false);
    public static final LongType LONG = new LongType(false);
    public static final FloatType FLOAT = new FloatType(false);
    public static final DoubleType DOUBLE = new DoubleType(false);
    public static final StringType STRING = new StringType(false);
    public static final EnumTypeBuilder ENUM = new EnumTypeBuilder();
    public static final UuidType UUID = new UuidType(false);
    public static final BigDecimalType BIG_DECIMAL = new BigDecimalType(false);
    public static final LocalDateType LOCAL_DATE = new LocalDateType(false);
    public static final LocalTimeType LOCAL_TIME = new LocalTimeType(false);
    public static final LocalDateTimeType LOCAL_DATE_TIME = new LocalDateTimeType(false);
    public static final InstantType INSTANT = new InstantType(false);
    public static final ListTypeBuilder LIST = new ListTypeBuilder();
    public static final SetTypeBuilder SET = new SetTypeBuilder();
    public static final MapTypeBuilder MAP = new MapTypeBuilder();

    public static <T> WriteRecordModelType<T> writeRecordModel(Class<T> entityClass) {
        return WriteRecordModelType.writeRecordModel(entityClass);
    }

    private FieldTypes() {
    }

}
