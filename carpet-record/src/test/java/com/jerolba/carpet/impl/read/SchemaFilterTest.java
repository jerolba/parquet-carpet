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
package com.jerolba.carpet.impl.read;

import static com.jerolba.carpet.FieldMatchingStrategy.FIELD_NAME;
import static com.jerolba.carpet.FieldMatchingStrategy.SNAKE_CASE;
import static org.apache.parquet.schema.ConversionPatterns.listOfElements;
import static org.apache.parquet.schema.ConversionPatterns.listType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.uuidType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.parquet.schema.ConversionPatterns;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.jerolba.carpet.CarpetMissingColumnException;
import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.annotation.Alias;
import com.jerolba.carpet.annotation.NotNull;
import com.jerolba.carpet.impl.read.ColumnToFieldMapper;
import com.jerolba.carpet.impl.read.SchemaFilter;
import com.jerolba.carpet.impl.read.SchemaValidation;

class SchemaFilterTest {

    private static final String ELEMENT = "element";
    private static final String MAP_VALUE = "value";
    private static final String MAP_KEY = "key";

    private final SchemaValidation defaultReadConfig = new SchemaValidation(true, false, false);
    private final SchemaValidation strictNumericConfig = new SchemaValidation(true, true, false);
    private final SchemaValidation dontFailOnMissingFields = new SchemaValidation(false, false, false);
    private final SchemaValidation failOnNullForPrimitives = new SchemaValidation(true, false, true);
    private final ColumnToFieldMapper defaultFieldMapper = new ColumnToFieldMapper(FIELD_NAME);

    @Nested
    class FieldInt32Conversion {

        @Test
        void fieldRequired() {
            Type field = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

            record PrimitiveInteger(int value) {
            }
            assertEquals(groupType, filter.project(PrimitiveInteger.class, groupType));

            record ObjectInteger(Integer value) {
            }
            assertEquals(groupType, filter.project(ObjectInteger.class, groupType));
        }

        @Test
        void fieldOptional() {
            Type field = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filterDefault = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            SchemaFilter filterFailOnNotNullable = new SchemaFilter(failOnNullForPrimitives, defaultFieldMapper);

            record PrimitiveInteger(int value) {
            }
            assertEquals(groupType, filterDefault.project(PrimitiveInteger.class, groupType));
            assertThrows(RecordTypeConversionException.class,
                    () -> filterFailOnNotNullable.project(PrimitiveInteger.class, groupType));

            record ObjectInteger(Integer value) {
            }
            assertEquals(groupType, filterDefault.project(ObjectInteger.class, groupType));
            assertEquals(groupType, filterFailOnNotNullable.project(ObjectInteger.class, groupType));
        }

        @Test
        void castToLongIsSupported() {
            Type field = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

            record PrimitiveLong(long value) {
            }
            assertEquals(groupType, filter.project(PrimitiveLong.class, groupType));

            record ObjectLong(Long value) {
            }
            assertEquals(groupType, filter.project(ObjectLong.class, groupType));
        }

        @Test
        void castToShortIsSupportedIfStrictNotActive() {
            Type field = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "value");
            GroupType groupType = new MessageType("foo", field);

            record PrimitiveShort(short value) {
            }
            record ObjectShort(Short value) {
            }

            SchemaFilter filterStrict = new SchemaFilter(strictNumericConfig, defaultFieldMapper);
            assertThrows(RecordTypeConversionException.class,
                    () -> filterStrict.project(PrimitiveShort.class, groupType));
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.project(ObjectShort.class, groupType));

            SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertEquals(groupType, filterNonStrict.project(PrimitiveShort.class, groupType));
            assertEquals(groupType, filterNonStrict.project(ObjectShort.class, groupType));
        }

        @Test
        void castToShortIsSupportedWithLogicalTypeInt16() {
            Type field = Types.primitive(PrimitiveTypeName.INT32, REQUIRED)
                    .as(intType(16, true))
                    .named("value");
            GroupType groupType = new MessageType("foo", field);

            record PrimitiveShort(short value) {
            }
            record ObjectShort(Short value) {
            }

            SchemaFilter filterStrict = new SchemaFilter(strictNumericConfig, defaultFieldMapper);
            assertEquals(groupType, filterStrict.project(PrimitiveShort.class, groupType));
            assertEquals(groupType, filterStrict.project(ObjectShort.class, groupType));

            SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertEquals(groupType, filterNonStrict.project(PrimitiveShort.class, groupType));
            assertEquals(groupType, filterNonStrict.project(ObjectShort.class, groupType));
        }

        @Test
        void castToByteIsSupportedIfStrictNotActive() {
            Type field = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "value");
            GroupType groupType = new MessageType("foo", field);

            record PrimitiveByte(byte value) {
            }
            record ObjectByte(Byte value) {
            }

            SchemaFilter filterStrict = new SchemaFilter(strictNumericConfig, defaultFieldMapper);
            assertThrows(RecordTypeConversionException.class,
                    () -> filterStrict.project(PrimitiveByte.class, groupType));
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.project(ObjectByte.class, groupType));

            SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertEquals(groupType, filterNonStrict.project(PrimitiveByte.class, groupType));
            assertEquals(groupType, filterNonStrict.project(ObjectByte.class, groupType));
        }

        @Test
        void castToByteIsSupportedWithLogicalTypeInt8() {
            Type field = Types.primitive(PrimitiveTypeName.INT32, REQUIRED)
                    .as(intType(8, true))
                    .named("value");
            GroupType groupType = new MessageType("foo", field);

            record PrimitiveByte(byte value) {
            }
            record ObjectByte(Byte value) {
            }

            SchemaFilter filterStrict = new SchemaFilter(strictNumericConfig, defaultFieldMapper);
            assertEquals(groupType, filterStrict.project(PrimitiveByte.class, groupType));
            assertEquals(groupType, filterStrict.project(ObjectByte.class, groupType));

            SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertEquals(groupType, filterNonStrict.project(PrimitiveByte.class, groupType));
            assertEquals(groupType, filterNonStrict.project(ObjectByte.class, groupType));
        }

    }

    @Nested
    class FieldInt64Conversion {

        @Test
        void fieldRequired() {
            Type field = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT64, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

            record PrimitiveLong(long value) {
            }
            assertEquals(groupType, filter.project(PrimitiveLong.class, groupType));

            record ObjectLong(Long value) {
            }
            assertEquals(groupType, filter.project(ObjectLong.class, groupType));
        }

        @Test
        void fieldOptional() {
            Type field = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT64, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filterDefault = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            SchemaFilter filterFailOnNotNullable = new SchemaFilter(failOnNullForPrimitives, defaultFieldMapper);

            record PrimitiveLong(long value) {
            }
            assertEquals(groupType, filterDefault.project(PrimitiveLong.class, groupType));
            assertThrows(RecordTypeConversionException.class,
                    () -> filterFailOnNotNullable.project(PrimitiveLong.class, groupType));

            record ObjectLong(Long value) {
            }
            assertEquals(groupType, filterDefault.project(PrimitiveLong.class, groupType));
            assertEquals(groupType, filterFailOnNotNullable.project(ObjectLong.class, groupType));
        }

        @Test
        void castToIntIsSupportedIfStrictNotActive() {
            Type field = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT64, "value");
            GroupType groupType = new MessageType("foo", field);

            record PrimitiveInt(int value) {
            }
            record ObjectInteger(Integer value) {
            }

            SchemaFilter filterStrict = new SchemaFilter(strictNumericConfig, defaultFieldMapper);
            assertThrows(RecordTypeConversionException.class,
                    () -> filterStrict.project(PrimitiveInt.class, groupType));
            assertThrows(RecordTypeConversionException.class,
                    () -> filterStrict.project(ObjectInteger.class, groupType));

            SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertEquals(groupType, filterNonStrict.project(PrimitiveInt.class, groupType));
            assertEquals(groupType, filterNonStrict.project(ObjectInteger.class, groupType));
        }

        @Test
        void castToShortIsSupportedIfStrictNotActive() {
            Type field = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT64, "value");
            GroupType groupType = new MessageType("foo", field);

            record PrimitiveShort(short value) {
            }
            record ObjectShort(Short value) {
            }

            SchemaFilter filterStrict = new SchemaFilter(strictNumericConfig, defaultFieldMapper);
            assertThrows(RecordTypeConversionException.class,
                    () -> filterStrict.project(PrimitiveShort.class, groupType));
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.project(ObjectShort.class, groupType));

            SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertEquals(groupType, filterNonStrict.project(PrimitiveShort.class, groupType));
            assertEquals(groupType, filterNonStrict.project(ObjectShort.class, groupType));
        }

        @Test
        void castToByteIsSupportedIfStrictNotActive() {
            Type field = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT64, "value");
            GroupType groupType = new MessageType("foo", field);

            record PrimitiveByte(byte value) {
            }
            record ObjectByte(Byte value) {
            }

            SchemaFilter filterStrict = new SchemaFilter(strictNumericConfig, defaultFieldMapper);
            assertThrows(RecordTypeConversionException.class,
                    () -> filterStrict.project(PrimitiveByte.class, groupType));
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.project(ObjectByte.class, groupType));

            SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertEquals(groupType, filterNonStrict.project(PrimitiveByte.class, groupType));
            assertEquals(groupType, filterNonStrict.project(ObjectByte.class, groupType));
        }

    }

    @Nested
    class FieldFloatConversion {

        @Test
        void fieldRequired() {
            Type field = new PrimitiveType(REQUIRED, PrimitiveTypeName.FLOAT, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

            record PrimitiveFloat(float value) {
            }
            assertEquals(groupType, filter.project(PrimitiveFloat.class, groupType));

            record ObjectFloat(Float value) {
            }
            assertEquals(groupType, filter.project(ObjectFloat.class, groupType));
        }

        @Test
        void fieldOptional() {
            Type field = new PrimitiveType(OPTIONAL, PrimitiveTypeName.FLOAT, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filterDefault = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            SchemaFilter filterFailOnNotNullable = new SchemaFilter(failOnNullForPrimitives, defaultFieldMapper);

            record PrimitiveFloat(float value) {
            }
            assertEquals(groupType, filterDefault.project(PrimitiveFloat.class, groupType));
            assertThrows(RecordTypeConversionException.class,
                    () -> filterFailOnNotNullable.project(PrimitiveFloat.class, groupType));

            record ObjectFloat(Float value) {
            }
            assertEquals(groupType, filterDefault.project(ObjectFloat.class, groupType));
            assertEquals(groupType, filterFailOnNotNullable.project(ObjectFloat.class, groupType));
        }

        @Test
        void castToDoubleIsSupported() {
            Type field = new PrimitiveType(REQUIRED, PrimitiveTypeName.FLOAT, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

            record PrimitiveDouble(double value) {
            }
            assertEquals(groupType, filter.project(PrimitiveDouble.class, groupType));

            record ObjectDouble(Double value) {
            }
            assertEquals(groupType, filter.project(ObjectDouble.class, groupType));
        }

    }

    @Nested
    class FieldDoubleConversion {

        @Test
        void fieldRequired() {
            Type field = new PrimitiveType(REQUIRED, PrimitiveTypeName.DOUBLE, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

            record PrimitiveDouble(double value) {
            }
            assertEquals(groupType, filter.project(PrimitiveDouble.class, groupType));

            record ObjectDouble(Double value) {
            }
            assertEquals(groupType, filter.project(ObjectDouble.class, groupType));
        }

        @Test
        void fieldOptional() {
            Type field = new PrimitiveType(OPTIONAL, PrimitiveTypeName.DOUBLE, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filterDefault = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            SchemaFilter filterFailOnNotNullable = new SchemaFilter(failOnNullForPrimitives, defaultFieldMapper);

            record PrimitiveDouble(double value) {
            }
            assertEquals(groupType, filterDefault.project(PrimitiveDouble.class, groupType));
            assertThrows(RecordTypeConversionException.class,
                    () -> filterFailOnNotNullable.project(PrimitiveDouble.class, groupType));

            record ObjectDouble(Double value) {
            }
            assertEquals(groupType, filterDefault.project(ObjectDouble.class, groupType));
            assertEquals(groupType, filterFailOnNotNullable.project(ObjectDouble.class, groupType));
        }

        @Test
        void castToFloatIsSupportedIfStrictNotActive() {
            Type field = new PrimitiveType(REQUIRED, PrimitiveTypeName.DOUBLE, "value");
            GroupType groupType = new MessageType("foo", field);

            record PrimitiveFloat(float value) {
            }
            record ObjectFloat(Float value) {
            }

            SchemaFilter filterStrict = new SchemaFilter(strictNumericConfig, defaultFieldMapper);
            assertThrows(RecordTypeConversionException.class,
                    () -> filterStrict.project(PrimitiveFloat.class, groupType));
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.project(ObjectFloat.class, groupType));

            SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertEquals(groupType, filterNonStrict.project(PrimitiveFloat.class, groupType));
            assertEquals(groupType, filterNonStrict.project(ObjectFloat.class, groupType));
        }

    }

    @Nested
    class FieldStringConversion {

        @Test
        void fieldRequired() {
            Type field = Types.primitive(BINARY, REQUIRED).as(stringType()).named("value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

            record NotNullString(@NotNull String value) {
            }
            assertEquals(groupType, filter.project(NotNullString.class, groupType));

            record NullableString(String value) {
            }
            assertEquals(groupType, filter.project(NullableString.class, groupType));
        }

        @Test
        void fieldOptional() {
            Type field = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filterDefault = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            SchemaFilter filterFailOnNotNullable = new SchemaFilter(failOnNullForPrimitives, defaultFieldMapper);

            record NotNullString(@NotNull String value) {
            }
            assertEquals(groupType, filterDefault.project(NotNullString.class, groupType));
            assertThrows(RecordTypeConversionException.class,
                    () -> filterFailOnNotNullable.project(NotNullString.class, groupType));

            record NullableString(String value) {
            }
            assertEquals(groupType, filterDefault.project(NullableString.class, groupType));
            assertEquals(groupType, filterFailOnNotNullable.project(NullableString.class, groupType));
        }

    }

    @Nested
    class FieldByteArrayConversion {

        @Test
        void fieldRequired() {
            Type field = Types.primitive(BINARY, REQUIRED).as(stringType()).named("value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

            record NotNullByteArray(@NotNull byte[] value) {
            }
            assertEquals(groupType, filter.project(NotNullByteArray.class, groupType));

            record NullableByteArray(byte[] value) {
            }
            assertEquals(groupType, filter.project(NullableByteArray.class, groupType));
        }

        @Test
        void fieldOptional() {
            Type field = Types.primitive(BINARY, OPTIONAL).named("value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filterDefault = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            SchemaFilter filterFailOnNotNullable = new SchemaFilter(failOnNullForPrimitives, defaultFieldMapper);

            record NotNullByteArray(@NotNull byte[] value) {
            }
            assertEquals(groupType, filterDefault.project(NotNullByteArray.class, groupType));
            assertThrows(RecordTypeConversionException.class,
                    () -> filterFailOnNotNullable.project(NotNullByteArray.class, groupType));

            record NullableByteArray(byte[] value) {
            }
            assertEquals(groupType, filterDefault.project(NullableByteArray.class, groupType));
            assertEquals(groupType, filterFailOnNotNullable.project(NullableByteArray.class, groupType));
        }

    }

    @Nested
    class FieldUuidConversion {

        @Test
        void fieldRequired() {
            Type field = Types.primitive(FIXED_LEN_BYTE_ARRAY, REQUIRED).as(uuidType())
                    .length(UUIDLogicalTypeAnnotation.BYTES)
                    .named("value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

            record NotNullUuid(@NotNull UUID value) {
            }
            assertEquals(groupType, filter.project(NotNullUuid.class, groupType));

            record NullableUuid(UUID value) {
            }
            assertEquals(groupType, filter.project(NullableUuid.class, groupType));
        }

        @Test
        void fieldOptional() {
            Type field = Types.primitive(FIXED_LEN_BYTE_ARRAY, OPTIONAL).as(uuidType())
                    .length(UUIDLogicalTypeAnnotation.BYTES)
                    .named("value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filterDefault = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            SchemaFilter filterFailOnNotNullable = new SchemaFilter(failOnNullForPrimitives, defaultFieldMapper);

            record NotNullUuid(@NotNull UUID value) {
            }
            assertEquals(groupType, filterDefault.project(NotNullUuid.class, groupType));
            assertThrows(RecordTypeConversionException.class,
                    () -> filterFailOnNotNullable.project(NotNullUuid.class, groupType));

            record NullableUuid(UUID value) {
            }
            assertEquals(groupType, filterDefault.project(NullableUuid.class, groupType));
            assertEquals(groupType, filterFailOnNotNullable.project(NullableUuid.class, groupType));
        }

        @Test
        void castToStringIsSupported() {
            Type field = Types.primitive(FIXED_LEN_BYTE_ARRAY, OPTIONAL).as(uuidType())
                    .length(UUIDLogicalTypeAnnotation.BYTES)
                    .named("value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filterDefault = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            SchemaFilter filterFailOnNotNullable = new SchemaFilter(failOnNullForPrimitives, defaultFieldMapper);

            record NullableString(String value) {
            }
            assertEquals(groupType, filterDefault.project(NullableString.class, groupType));
            assertEquals(groupType, filterFailOnNotNullable.project(NullableString.class, groupType));
        }
    }

    @Nested
    class FieldEnumConversion {

        enum Category {
            one, two
        }

        @Test
        void fieldRequired() {
            Type field = Types.primitive(BINARY, REQUIRED).as(enumType()).named("value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

            record NotNullEnum(@NotNull Category value) {
            }
            assertEquals(groupType, filter.project(NotNullEnum.class, groupType));

            record NullableEnum(Category value) {
            }
            assertEquals(groupType, filter.project(NullableEnum.class, groupType));
        }

        @Test
        void fieldOptional() {
            Type field = Types.primitive(BINARY, OPTIONAL).as(enumType()).named("value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filterDefault = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            SchemaFilter filterFailOnNotNullable = new SchemaFilter(failOnNullForPrimitives, defaultFieldMapper);

            record NotNullEnum(@NotNull Category value) {
            }
            assertEquals(groupType, filterDefault.project(NotNullEnum.class, groupType));
            assertThrows(RecordTypeConversionException.class,
                    () -> filterFailOnNotNullable.project(NotNullEnum.class, groupType));

            record NullableEnum(Category value) {
            }
            assertEquals(groupType, filterDefault.project(NullableEnum.class, groupType));
            assertEquals(groupType, filterFailOnNotNullable.project(NullableEnum.class, groupType));
        }

        @Test
        void castToStringIsSupported() {
            Type field = Types.primitive(BINARY, REQUIRED).as(enumType()).named("value");
            GroupType groupType = new MessageType("foo", field);

            record CastToString(String value) {
            }

            SchemaFilter filterStrict = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertEquals(groupType, filterStrict.project(CastToString.class, groupType));
        }

        @Test
        void stringCastToEnumIsSupported() {
            Type field = Types.primitive(BINARY, REQUIRED).as(stringType()).named("value");
            GroupType groupType = new MessageType("foo", field);

            record CastToEnum(Category value) {
            }

            SchemaFilter filterStrict = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertEquals(groupType, filterStrict.project(CastToEnum.class, groupType));
        }

    }

    @Nested
    class ParquetFieldsMatchingRecordFields {

        @Test
        void fieldsMatch() {
            Type field1 = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type field2 = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType groupType = new MessageType("foo", field1, field2);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

            record AllPresent(String name, int age) {
            }
            assertEquals(groupType, filter.project(AllPresent.class, groupType));
        }

        @Test
        void ifSchemaHasMoreFieldsThanNeededAreFilteredInProjection() {
            Type field1 = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type field2 = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType groupType = new MessageType("foo", field1, field2);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

            record OnlyName(String name) {
            }
            GroupType expectedName = new MessageType("foo", field1);
            assertEquals(expectedName, filter.project(OnlyName.class, groupType));

            record OnlyAge(int age) {
            }
            GroupType expectedAge = new MessageType("foo", field2);
            assertEquals(expectedAge, filter.project(OnlyAge.class, groupType));
        }

        @Test
        void ifSchemaHasLessFieldsThanNeededProjectionFails() {
            Type field1 = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type field2 = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType groupType = new MessageType("foo", field1, field2);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

            record MoreThanExisting(String name, int age, boolean active) {
            }

            assertThrows(CarpetMissingColumnException.class, () -> filter.project(MoreThanExisting.class, groupType));
        }

        @Test
        void ifSchemaHasLessFieldsThanNeededButSupportsItFieldsAreNulled() {
            Type field1 = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type field2 = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType groupType = new MessageType("foo", field1, field2);

            record MoreThanExisting(String name, int age, boolean active) {
            }

            SchemaFilter filter = new SchemaFilter(dontFailOnMissingFields, defaultFieldMapper);
            GroupType expectedAge = new MessageType("foo", field1, field2);
            assertEquals(expectedAge, filter.project(MoreThanExisting.class, groupType));
        }

    }

    @Nested
    class Composite {

        @Test
        void compositeChild() {
            Type field1 = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type childField1 = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            Type childField2 = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType childGroupType = new GroupType(OPTIONAL, "child", childField1, childField2);
            GroupType groupType = new MessageType("foo", field1, childGroupType);

            record Child(String id, int age) {
            }
            record CompositeMain(String name, Child child) {
            }

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertEquals(groupType, filter.project(CompositeMain.class, groupType));
        }

        @Test
        void ifSchemaHasMoreFieldsThanNeededAreFilteredInProjection() {
            Type field1 = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type childField1 = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            Type childField2 = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
            Type childField3 = new PrimitiveType(REQUIRED, PrimitiveTypeName.BOOLEAN, "active");
            GroupType childGroupType = new GroupType(OPTIONAL, "child", childField1, childField2,
                    childField3);
            GroupType groupType = new MessageType("foo", field1, childGroupType);

            record Child(String id, int age) {
            }
            record CompositeMain(String name, Child child) {
            }

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

            GroupType expectedChildGroupType = new GroupType(OPTIONAL, "child", childField1, childField2);
            GroupType expectedGroup = new MessageType("foo", field1, expectedChildGroupType);
            assertEquals(expectedGroup, filter.project(CompositeMain.class, groupType));
        }

        @Test
        void ifSchemaHasLessFieldsThanNeededProjectionFails() {
            Type field1 = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type childField1 = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            Type childField2 = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
            Type childField3 = new PrimitiveType(REQUIRED, PrimitiveTypeName.BOOLEAN, "active");
            GroupType childGroupType = new GroupType(OPTIONAL, "child", childField1, childField2,
                    childField3);
            GroupType groupType = new MessageType("foo", field1, childGroupType);

            record Child(String id, int age, boolean active, double amount) {
            }
            record CompositeMain(String name, Child child) {
            }

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertThrows(CarpetMissingColumnException.class, () -> filter.project(CompositeMain.class, groupType));
        }

        @Test
        void ifSchemaHasLessFieldsThanNeededButSupportsItFieldsAreNulled() {
            Type field1 = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type childField1 = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            Type childField2 = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
            Type childField3 = new PrimitiveType(REQUIRED, PrimitiveTypeName.BOOLEAN, "active");
            GroupType childGroupType = new GroupType(OPTIONAL, "child", childField1, childField2,
                    childField3);
            GroupType groupType = new MessageType("foo", field1, childGroupType);

            record Child(String id, int age, boolean active, double amount) {
            }
            record CompositeMain(String name, Child child) {
            }

            SchemaFilter filter = new SchemaFilter(dontFailOnMissingFields, defaultFieldMapper);

            assertEquals(groupType, filter.project(CompositeMain.class, groupType));
        }
    }

    @Nested
    class SimpleClassesAreNotSupported {

        private static class NormalClass {
            private final String id;

            public NormalClass(String id) {
                this.id = id;
            }

            public String getId() {
                return id;
            }

        }

        @Test
        void javaBeanCanNotBeDeserialized() {
            Type field1 = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            GroupType groupType = new MessageType("foo", field1);

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertThrows(RecordTypeConversionException.class, () -> filter.project(NormalClass.class, groupType));
        }

        @Test
        void javaBeanCanNotBePartOfRecord() {
            Type field1 = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            Type childField1 = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            GroupType childGroupType = new GroupType(OPTIONAL, "child", childField1);
            GroupType groupType = new MessageType("foo", field1, childGroupType);

            record ParentClass(String id, NormalClass child) {
            }

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertThrows(RecordTypeConversionException.class, () -> filter.project(ParentClass.class, groupType));

            record ChildRecord(String id) {
            }
            record ParentRecord(String id, ChildRecord child) {
            }
            assertEquals(groupType, filter.project(ParentRecord.class, groupType));
        }

        @Test
        void bigIntegerIsNotSupported() {
            Type field1 = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT64, "id");
            GroupType groupType = new MessageType("foo", field1);

            record BigIntegerRecord(BigInteger id) {
            }
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertThrows(RecordTypeConversionException.class, () -> filter.project(BigIntegerRecord.class, groupType));
        }

        @Test
        void bigDecimalIsNotSupported() {
            Type field1 = new PrimitiveType(REQUIRED, PrimitiveTypeName.DOUBLE, "value");
            GroupType groupType = new MessageType("foo", field1);

            record BigDecimalRecord(BigDecimal value) {
            }
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertThrows(RecordTypeConversionException.class, () -> filter.project(BigDecimalRecord.class, groupType));
        }

    }

    @Nested
    class Collections {

        Type fieldId = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
        Type fieldName = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
        Type fieldAge = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
        Type fieldActive = new PrimitiveType(REQUIRED, PrimitiveTypeName.BOOLEAN, "active");

        @Nested
        class OneLevelCollection {

            @Test
            void integerCollection() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.INT32, "ids");
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record LevelOnePrimitive(String name, List<Integer> ids) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filter.project(LevelOnePrimitive.class, groupType));
            }

            @Test
            void shortCollection() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.INT32, "ids");
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record LevelOnePrimitive(String name, List<Short> ids) {
                }

                SchemaFilter filter = new SchemaFilter(strictNumericConfig, defaultFieldMapper);
                assertThrows(RecordTypeConversionException.class,
                        () -> filter.project(LevelOnePrimitive.class, groupType));

                SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filterNonStrict.project(LevelOnePrimitive.class, groupType));
            }

            @Test
            void byteCollection() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.INT32, "ids");
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record LevelOnePrimitive(String name, List<Byte> ids) {
                }

                SchemaFilter filter = new SchemaFilter(strictNumericConfig, defaultFieldMapper);
                assertThrows(RecordTypeConversionException.class,
                        () -> filter.project(LevelOnePrimitive.class, groupType));

                SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filterNonStrict.project(LevelOnePrimitive.class, groupType));
            }

            @Test
            void longCollection() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.INT64, "ids");
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record LevelOnePrimitive(String name, List<Long> ids) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filter.project(LevelOnePrimitive.class, groupType));
            }

            @Test
            void floatCollection() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.FLOAT, "values");
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record LevelOnePrimitive(String name, List<Float> values) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filter.project(LevelOnePrimitive.class, groupType));
            }

            @Test
            void floatFromDoubleCollection() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.DOUBLE, "values");
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record LevelOnePrimitive(String name, List<Float> values) {
                }

                SchemaFilter filter = new SchemaFilter(strictNumericConfig, defaultFieldMapper);
                assertThrows(RecordTypeConversionException.class,
                        () -> filter.project(LevelOnePrimitive.class, groupType));

                SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filterNonStrict.project(LevelOnePrimitive.class, groupType));
            }

            @Test
            void doubleCollection() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.DOUBLE, "values");
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record LevelOnePrimitive(String name, List<Double> values) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filter.project(LevelOnePrimitive.class, groupType));
            }

            @Test
            void stringCollection() {
                Type repeated = Types.primitive(BINARY, REPEATED).as(stringType()).named("ids");
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record LevelOnePrimitive(String name, List<String> ids) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filter.project(LevelOnePrimitive.class, groupType));
            }

            enum Category {
                one, two
            }

            @Test
            void enumCollection() {
                Type repeated = Types.primitive(BINARY, REPEATED).as(enumType()).named("categories");
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record LevelOnePrimitive(String name, List<Category> categories) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filter.project(LevelOnePrimitive.class, groupType));
            }

            @Test
            void enumToStringCollection() {
                Type repeated = Types.primitive(BINARY, REPEATED).as(enumType()).named("categories");
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record LevelOnePrimitive(String name, List<String> categories) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filter.project(LevelOnePrimitive.class, groupType));
            }

            @Test
            void compositeCollection() {
                GroupType repeated = new GroupType(REPEATED, "child", fieldId, fieldAge);
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record Child(String id, int age) {
                }
                record LevelOneComposite(String name, List<Child> child) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filter.project(LevelOneComposite.class, groupType));
            }

            @Test
            void compositeCollectionMatchFields() {
                GroupType repeated = new GroupType(REPEATED, "child", fieldId, fieldAge, fieldActive);
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record Child(String id, int age) {
                }
                record LevelOneComposite(String name, List<Child> child) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

                GroupType repeatedExoected = new GroupType(REPEATED, "child", fieldId, fieldAge);
                GroupType groupTypeExpected = new MessageType("foo", fieldName, repeatedExoected);
                assertEquals(groupTypeExpected, filter.project(LevelOneComposite.class, groupType));
            }

            @Test
            void nestedCollectionsAreNotSupported() {
                GroupType repeatedChild = new GroupType(REPEATED, "nested", fieldId, fieldAge);
                GroupType repeated = new GroupType(REPEATED, "list", repeatedChild);
                GroupType groupType = new MessageType("foo", fieldName, repeated);
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

                record Child(String id, int age) {
                }
                record NestedCollections(String name, List<List<Child>> list) {
                }

                assertThrows(RecordTypeConversionException.class,
                        () -> filter.project(NestedCollections.class, groupType));

                record Nested(List<Child> nested) {
                }
                record ValidLevelOneComposite(String name, List<Nested> list) {
                }
                assertEquals(groupType, filter.project(ValidLevelOneComposite.class, groupType));
            }

            @Test
            void collectionCanBeFiltered() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.INT32, "ids");
                GroupType groupType = new MessageType("foo", fieldName, fieldActive, fieldId, repeated);

                record LevelOnePrimitive(String id, String name, boolean active) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                GroupType expected = new MessageType("foo", fieldName, fieldActive, fieldId);
                assertEquals(expected, filter.project(LevelOnePrimitive.class, groupType));
            }
        }

        @Nested
        @SuppressWarnings("deprecation")
        class TwoLevelCollection {

            @Test
            void integerCollection() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.INT32, ELEMENT);
                GroupType listType = listType(OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<Integer> ids) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filter.project(LevelTwoPrimitive.class, groupType));
            }

            @Test
            void shortCollection() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.INT32, ELEMENT);
                GroupType listType = listType(OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<Short> ids) {
                }

                SchemaFilter filter = new SchemaFilter(strictNumericConfig, defaultFieldMapper);
                assertThrows(RecordTypeConversionException.class,
                        () -> filter.project(LevelTwoPrimitive.class, groupType));

                SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filterNonStrict.project(LevelTwoPrimitive.class, groupType));
            }

            @Test
            void byteCollection() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.INT32, ELEMENT);
                GroupType listType = listType(OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<Byte> ids) {
                }

                SchemaFilter filter = new SchemaFilter(strictNumericConfig, defaultFieldMapper);
                assertThrows(RecordTypeConversionException.class,
                        () -> filter.project(LevelTwoPrimitive.class, groupType));

                SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filterNonStrict.project(LevelTwoPrimitive.class, groupType));
            }

            @Test
            void longCollection() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.INT64, ELEMENT);
                GroupType listType = listType(OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<Long> ids) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filter.project(LevelTwoPrimitive.class, groupType));
            }

            @Test
            void floatCollection() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.FLOAT, ELEMENT);
                GroupType listType = listType(OPTIONAL, "values", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<Float> values) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filter.project(LevelTwoPrimitive.class, groupType));
            }

            @Test
            void floatFromDoubleCollection() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.DOUBLE, ELEMENT);
                GroupType listType = listType(OPTIONAL, "values", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<Float> values) {
                }

                SchemaFilter filter = new SchemaFilter(strictNumericConfig, defaultFieldMapper);
                assertThrows(RecordTypeConversionException.class,
                        () -> filter.project(LevelTwoPrimitive.class, groupType));

                SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filterNonStrict.project(LevelTwoPrimitive.class, groupType));
            }

            @Test
            void doubleCollection() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.DOUBLE, ELEMENT);
                GroupType listType = listType(OPTIONAL, "values", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<Double> values) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filter.project(LevelTwoPrimitive.class, groupType));
            }

            @Test
            void stringCollection() {
                Type repeated = Types.primitive(BINARY, REPEATED).as(stringType()).named(ELEMENT);
                GroupType listType = listType(OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelOnePrimitive(String name, List<String> ids) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filter.project(LevelOnePrimitive.class, groupType));
            }

            enum Category {
                one, two
            }

            @Test
            void enumCollection() {
                Type repeated = Types.primitive(BINARY, REPEATED).as(enumType()).named(ELEMENT);
                GroupType listType = listType(OPTIONAL, "categories", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelOnePrimitive(String name, List<Category> categories) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filter.project(LevelOnePrimitive.class, groupType));
            }

            @Test
            void enumToStringCollection() {
                Type repeated = Types.primitive(BINARY, REPEATED).as(enumType()).named(ELEMENT);
                GroupType listType = listType(OPTIONAL, "categories", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<String> categories) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filter.project(LevelTwoPrimitive.class, groupType));
            }

            @Test
            void compositeCollection() {
                GroupType repeated = new GroupType(REPEATED, "child", fieldId, fieldAge);
                GroupType listType = listType(OPTIONAL, "child", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record Child(String id, int age) {
                }
                record LevelTwoComposite(String name, List<Child> child) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filter.project(LevelTwoComposite.class, groupType));
            }

            @Test
            void compositeCollectionMatchFields() {
                GroupType repeated = new GroupType(REPEATED, "child", fieldId, fieldAge, fieldActive);
                GroupType listType = listType(OPTIONAL, "child", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record Child(String id, int age) {
                }
                record LevelTwoComposite(String name, List<Child> child) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

                GroupType repeatedExpected = new GroupType(REPEATED, "child", fieldId, fieldAge);
                GroupType listTypeExpected = listType(OPTIONAL, "child", repeatedExpected);
                GroupType groupTypeExpected = new MessageType("foo", fieldName, listTypeExpected);
                assertEquals(groupTypeExpected, filter.project(LevelTwoComposite.class, groupType));
            }

            @Test
            void nestedCollectionsWithPrimitivesAreSupported() {
                Type repeatedChild = new PrimitiveType(REPEATED, PrimitiveTypeName.INT32, ELEMENT);
                GroupType listType = listType(REPEATED, ELEMENT, repeatedChild);
                GroupType repeated = listType(OPTIONAL, "values", listType);
                GroupType groupType = new MessageType("foo", fieldName, repeated);
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

                record NestedCollections(String name, List<List<Integer>> values) {
                }
                assertEquals(groupType, filter.project(NestedCollections.class, groupType));

                record Nested(List<Integer> nested) {
                }
                record ValidLevelTwoComposite(String name, List<Nested> list) {
                }
                assertThrows(CarpetMissingColumnException.class,
                        () -> filter.project(ValidLevelTwoComposite.class, groupType));
            }

            @Test
            void nestedCollectionsWithGroupAreSupported() {
                GroupType repeatedChild = new GroupType(REPEATED, ELEMENT, fieldId, fieldAge);
                GroupType listType = listType(REPEATED, ELEMENT, repeatedChild);
                GroupType repeated = listType(OPTIONAL, "values", listType);
                GroupType groupType = new MessageType("foo", fieldName, repeated);
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

                record Child(String id, int age) {
                }
                record NestedCollections(String name, List<List<Child>> values) {
                }
                assertEquals(groupType, filter.project(NestedCollections.class, groupType));

                record Nested(List<Child> nested) {
                }
                record ValidLevelTwoComposite(String name, List<Nested> list) {
                }
                assertThrows(CarpetMissingColumnException.class,
                        () -> filter.project(ValidLevelTwoComposite.class, groupType));
            }

            @Test
            void collectionCanBeFiltered() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.INT32, ELEMENT);
                GroupType listType = listType(OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, fieldActive, fieldId, listType);

                record LevelTwoPrimitive(String id, String name, boolean active) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                GroupType expected = new MessageType("foo", fieldName, fieldActive, fieldId);
                assertEquals(expected, filter.project(LevelTwoPrimitive.class, groupType));
            }

            @Test
            void nestedMapsAreSupported() {
                Type key = Types.primitive(BINARY, REQUIRED).as(stringType()).named(MAP_KEY);
                Type value = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, MAP_VALUE);
                Type mapType = Types.map(REPEATED).key(key).value(value).named(ELEMENT);
                GroupType repeated = listType(OPTIONAL, "values", mapType);
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

                record NestedMap(String name, List<Map<String, Integer>> values) {
                }
                assertEquals(groupType, filter.project(NestedMap.class, groupType));
            }
        }

        @Nested
        class ThreeLevelCollection {

            @Test
            void integerCollection() {
                Type repeated = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, ELEMENT);
                GroupType listType = listOfElements(OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelThreePrimitive(String name, List<Integer> ids) {
                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filter.project(LevelThreePrimitive.class, groupType));
            }

            @Test
            void shortCollection() {
                Type repeated = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, ELEMENT);
                GroupType listType = listOfElements(OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelThreePrimitive(String name, List<Short> ids) {
                }
                SchemaFilter filter = new SchemaFilter(strictNumericConfig, defaultFieldMapper);
                assertThrows(RecordTypeConversionException.class,
                        () -> filter.project(LevelThreePrimitive.class, groupType));

                SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filterNonStrict.project(LevelThreePrimitive.class, groupType));
            }

            @Test
            void byteCollection() {
                Type repeated = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, ELEMENT);
                GroupType listType = listOfElements(OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelThreePrimitive(String name, List<Byte> ids) {
                }
                SchemaFilter filter = new SchemaFilter(strictNumericConfig, defaultFieldMapper);
                assertThrows(RecordTypeConversionException.class,
                        () -> filter.project(LevelThreePrimitive.class, groupType));

                SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filterNonStrict.project(LevelThreePrimitive.class, groupType));
            }

            @Test
            void longCollection() {
                Type repeated = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT64, ELEMENT);
                GroupType listType = listOfElements(OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelThreePrimitive(String name, List<Long> ids) {
                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filter.project(LevelThreePrimitive.class, groupType));
            }

            @Test
            void floatCollection() {
                Type repeated = new PrimitiveType(OPTIONAL, PrimitiveTypeName.FLOAT, ELEMENT);
                GroupType listType = listOfElements(OPTIONAL, "values", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelThreePrimitive(String name, List<Float> values) {
                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filter.project(LevelThreePrimitive.class, groupType));
            }

            @Test
            void floatFromDoubleCollection() {
                Type repeated = new PrimitiveType(OPTIONAL, PrimitiveTypeName.DOUBLE, ELEMENT);
                GroupType listType = listOfElements(OPTIONAL, "values", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelThreePrimitive(String name, List<Float> values) {
                }
                SchemaFilter filter = new SchemaFilter(strictNumericConfig, defaultFieldMapper);
                assertThrows(RecordTypeConversionException.class,
                        () -> filter.project(LevelThreePrimitive.class, groupType));

                SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filterNonStrict.project(LevelThreePrimitive.class, groupType));
            }

            @Test
            void doubleCollection() {
                Type repeated = new PrimitiveType(OPTIONAL, PrimitiveTypeName.DOUBLE, ELEMENT);
                GroupType listType = listOfElements(OPTIONAL, "values", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelThreePrimitive(String name, List<Double> values) {
                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filter.project(LevelThreePrimitive.class, groupType));
            }

            @Test
            void stringCollection() {
                Type repeated = Types.primitive(BINARY, OPTIONAL).as(stringType()).named(ELEMENT);
                GroupType listType = listOfElements(OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelThreePrimitive(String name, List<String> ids) {
                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filter.project(LevelThreePrimitive.class, groupType));
            }

            enum Category {
                one, two
            }

            @Test
            void enumCollection() {
                Type repeated = Types.primitive(BINARY, OPTIONAL).as(enumType()).named(ELEMENT);
                GroupType listType = listOfElements(OPTIONAL, "categories", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelThreePrimitive(String name, List<Category> categories) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filter.project(LevelThreePrimitive.class, groupType));
            }

            @Test
            void enumToStringCollection() {
                Type repeated = Types.primitive(BINARY, OPTIONAL).as(enumType()).named(ELEMENT);
                GroupType listType = listOfElements(OPTIONAL, "categories", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelThreePrimitive(String name, List<String> categories) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filter.project(LevelThreePrimitive.class, groupType));
            }

            @Test
            void compositeCollection() {
                GroupType repeated = new GroupType(OPTIONAL, ELEMENT, fieldId, fieldAge);
                GroupType listType = listOfElements(OPTIONAL, "child", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record Child(String id, int age) {
                }
                record LevelThreeComposite(String name, List<Child> child) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                assertEquals(groupType, filter.project(LevelThreeComposite.class, groupType));
            }

            @Test
            void compositeCollectionMatchFields() {
                GroupType repeated = new GroupType(OPTIONAL, ELEMENT, fieldId, fieldAge, fieldActive);
                GroupType listType = listOfElements(OPTIONAL, "child", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record Child(String id, int age) {
                }
                record LevelThreeComposite(String name, List<Child> child) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

                GroupType repeatedExpected = new GroupType(OPTIONAL, ELEMENT, fieldId, fieldAge);
                GroupType listTypeExpected = listOfElements(OPTIONAL, "child", repeatedExpected);
                GroupType groupTypeExpected = new MessageType("foo", fieldName, listTypeExpected);
                assertEquals(groupTypeExpected, filter.project(LevelThreeComposite.class, groupType));
            }

            @Test
            void nestedCollectionsWithPrimitivesAreSupported() {
                Type repeatedChild = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, ELEMENT);
                GroupType listType = listOfElements(REPEATED, ELEMENT, repeatedChild);
                GroupType repeated = listOfElements(OPTIONAL, "values", listType);
                GroupType groupType = new MessageType("foo", fieldName, repeated);
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

                record NestedCollections(String name, List<List<Integer>> values) {
                }
                assertEquals(groupType, filter.project(NestedCollections.class, groupType));

                record Nested(List<Integer> nested) {
                }
                record ValidLevelThreeComposite(String name, List<Nested> list) {
                }
                assertThrows(CarpetMissingColumnException.class,
                        () -> filter.project(ValidLevelThreeComposite.class, groupType));
            }

            @Test
            void nestedCollectionsWithGroupAreSupported() {
                GroupType repeatedChild = new GroupType(OPTIONAL, ELEMENT, fieldId, fieldAge);
                GroupType listType = listOfElements(REPEATED, ELEMENT, repeatedChild);
                GroupType repeated = listOfElements(OPTIONAL, "values", listType);
                GroupType groupType = new MessageType("foo", fieldName, repeated);
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

                record Child(String id, int age) {
                }
                record NestedCollections(String name, List<List<Child>> values) {
                }
                assertEquals(groupType, filter.project(NestedCollections.class, groupType));

                record Nested(List<Child> nested) {
                }
                record ValidLevelThreeComposite(String name, List<Nested> values) {
                }
                assertThrows(CarpetMissingColumnException.class,
                        () -> filter.project(ValidLevelThreeComposite.class, groupType));
            }

            @Test
            void collectionCanBeFiltered() {
                Type repeated = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, ELEMENT);
                GroupType listType = listOfElements(OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, fieldId, fieldActive, listType);

                record LevelThreePrimitive(String id, String name, boolean active) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                GroupType expected = new MessageType("foo", fieldName, fieldId, fieldActive);
                assertEquals(expected, filter.project(LevelThreePrimitive.class, groupType));
            }

            @Test
            void nestedMapsAreSupported() {
                Type key = Types.primitive(BINARY, REQUIRED).as(stringType()).named(MAP_KEY);
                Type value = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, MAP_VALUE);
                Type mapType = Types.map(REPEATED).key(key).value(value).named(ELEMENT);
                GroupType repeated = listOfElements(OPTIONAL, "values", mapType);
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

                record NestedMap(String name, List<Map<String, Integer>> values) {
                }
                assertEquals(groupType, filter.project(NestedMap.class, groupType));
            }
        }

    }

    @Nested
    class Maps {

        Type fieldId = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
        Type fieldName = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
        Type fieldAge = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
        Type fieldActive = new PrimitiveType(REQUIRED, PrimitiveTypeName.BOOLEAN, "active");

        @Nested
        class WellFormedMap {

            @Nested
            class StringKey {

                Type mapKey = Types.primitive(BINARY, REQUIRED).as(stringType()).named(MAP_KEY);

                @Test
                void integerValue() {
                    Type value = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, MAP_VALUE);
                    Type mapType = Types.map(OPTIONAL).key(mapKey).value(value).named("ids");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapValuePrimitive(String name, Map<String, Integer> ids) {
                    }

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                    assertEquals(groupType, filter.project(MapValuePrimitive.class, groupType));
                }

                @Test
                void shortValue() {
                    Type value = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, MAP_VALUE);
                    Type mapType = Types.map(OPTIONAL).key(mapKey).value(value).named("ids");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapValuePrimitive(String name, Map<String, Short> ids) {
                    }

                    SchemaFilter filter = new SchemaFilter(strictNumericConfig, defaultFieldMapper);
                    assertThrows(RecordTypeConversionException.class,
                            () -> filter.project(MapValuePrimitive.class, groupType));

                    SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                    assertEquals(groupType, filterNonStrict.project(MapValuePrimitive.class, groupType));
                }

                @Test
                void byteValue() {
                    Type value = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, MAP_VALUE);
                    Type mapType = Types.map(OPTIONAL).key(mapKey).value(value).named("ids");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapValuePrimitive(String name, Map<String, Byte> ids) {
                    }

                    SchemaFilter filter = new SchemaFilter(strictNumericConfig, defaultFieldMapper);
                    assertThrows(RecordTypeConversionException.class,
                            () -> filter.project(MapValuePrimitive.class, groupType));

                    SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                    assertEquals(groupType, filterNonStrict.project(MapValuePrimitive.class, groupType));
                }

                @Test
                void longValue() {
                    Type value = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT64, MAP_VALUE);
                    Type mapType = Types.map(OPTIONAL).key(mapKey).value(value).named("ids");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapValuePrimitive(String name, Map<String, Long> ids) {
                    }

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                    assertEquals(groupType, filter.project(MapValuePrimitive.class, groupType));
                }

                @Test
                void floatValue() {
                    Type value = new PrimitiveType(OPTIONAL, PrimitiveTypeName.FLOAT, MAP_VALUE);
                    Type mapType = Types.map(OPTIONAL).key(mapKey).value(value).named("values");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapValuePrimitive(String name, Map<String, Float> values) {
                    }

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                    assertEquals(groupType, filter.project(MapValuePrimitive.class, groupType));
                }

                @Test
                void floatFromDoubleValue() {
                    Type value = new PrimitiveType(OPTIONAL, PrimitiveTypeName.DOUBLE, MAP_VALUE);
                    Type mapType = Types.map(OPTIONAL).key(mapKey).value(value).named("values");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapValuePrimitive(String name, Map<String, Float> values) {
                    }

                    SchemaFilter filter = new SchemaFilter(strictNumericConfig, defaultFieldMapper);
                    assertThrows(RecordTypeConversionException.class,
                            () -> filter.project(MapValuePrimitive.class, groupType));

                    SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                    assertEquals(groupType, filterNonStrict.project(MapValuePrimitive.class, groupType));
                }

                @Test
                void doubleValue() {
                    Type value = new PrimitiveType(OPTIONAL, PrimitiveTypeName.DOUBLE, MAP_VALUE);
                    Type mapType = Types.map(OPTIONAL).key(mapKey).value(value).named("values");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapValuePrimitive(String name, Map<String, Double> values) {
                    }

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                    assertEquals(groupType, filter.project(MapValuePrimitive.class, groupType));
                }

                @Test
                void stringValue() {
                    Type value = Types.primitive(BINARY, OPTIONAL).as(stringType()).named(MAP_VALUE);
                    Type mapType = Types.map(OPTIONAL).key(mapKey).value(value).named("ids");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapValuePrimitive(String name, Map<String, String> ids) {
                    }

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                    assertEquals(groupType, filter.project(MapValuePrimitive.class, groupType));
                }

                enum Category {
                    one, two
                }

                @Test
                void enumValue() {
                    Type value = Types.primitive(BINARY, OPTIONAL).as(enumType()).named(MAP_VALUE);
                    Type mapType = Types.map(OPTIONAL).key(mapKey).value(value).named("categories");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapValuePrimitive(String name, Map<String, Category> categories) {
                    }

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                    assertEquals(groupType, filter.project(MapValuePrimitive.class, groupType));
                }

                @Test
                void enumToStringValue() {
                    Type value = Types.primitive(BINARY, OPTIONAL).as(enumType()).named(MAP_VALUE);
                    Type mapType = Types.map(OPTIONAL).key(mapKey).value(value).named("categories");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapValuePrimitive(String name, Map<String, String> categories) {
                    }

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                    assertEquals(groupType, filter.project(MapValuePrimitive.class, groupType));
                }

                @Test
                void compositeValue() {
                    Type value = new GroupType(OPTIONAL, MAP_VALUE, fieldId, fieldAge);
                    Type mapType = Types.map(OPTIONAL).key(mapKey).value(value).named("child");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record Child(String id, int age) {
                    }
                    record MapValueComposite(String name, Map<String, Child> child) {
                    }

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                    assertEquals(groupType, filter.project(MapValueComposite.class, groupType));
                }

                @Test
                void compositeValueMatchFields() {
                    Type value = new GroupType(OPTIONAL, MAP_VALUE, fieldId, fieldAge, fieldActive);
                    Type mapType = Types.map(OPTIONAL).key(mapKey).value(value).named("child");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record Child(String id, int age) {
                    }
                    record MapValueComposite(String name, Map<String, Child> child) {
                    }

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

                    Type valueExpected = new GroupType(OPTIONAL, MAP_VALUE, fieldId, fieldAge);
                    Type mapTypeExpected = Types.map(OPTIONAL).key(mapKey).value(valueExpected)
                            .named("child");
                    GroupType groupTypeExpected = new MessageType("foo", fieldName, mapTypeExpected);
                    assertEquals(groupTypeExpected, filter.project(MapValueComposite.class, groupType));
                }

                @Test
                void nestedMapValueWithPrimitivesAreSupported() {
                    Type value = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, MAP_VALUE);
                    Type innerMapType = Types.map(OPTIONAL).key(mapKey).value(value).named(MAP_VALUE);
                    Type firstMapType = Types.map(OPTIONAL).key(mapKey).value(innerMapType).named("values");
                    GroupType groupType = new MessageType("foo", fieldName, firstMapType);

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

                    record NestedMaps(String name, Map<String, Map<String, Integer>> values) {
                    }
                    assertEquals(groupType, filter.project(NestedMaps.class, groupType));

                    record Nested(Map<String, Integer> nested) {
                    }
                    record InvalidNestedComposite(String name, Map<String, Nested> list) {
                    }
                    assertThrows(CarpetMissingColumnException.class,
                            () -> filter.project(InvalidNestedComposite.class, groupType));
                }

                @Test
                void nestedMapValueWithGroupAreSupported() {
                    GroupType groupChild = new GroupType(OPTIONAL, ELEMENT, fieldId, fieldAge);
                    GroupType innerMapType = Types.map(OPTIONAL).key(mapKey).value(groupChild).named(MAP_VALUE);
                    GroupType firstMapType = Types.map(OPTIONAL).key(mapKey).value(innerMapType).named("values");
                    GroupType groupType = new MessageType("foo", fieldName, firstMapType);
                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

                    record Child(String id, int age) {
                    }
                    record NestedMaps(String name, Map<String, Map<String, Child>> values) {
                    }
                    assertEquals(groupType, filter.project(NestedMaps.class, groupType));

                    record Nested(Map<String, Child> nested) {
                    }
                    record InvalidNestedComposite(String name, Map<String, Nested> list) {
                    }
                    assertThrows(CarpetMissingColumnException.class,
                            () -> filter.project(InvalidNestedComposite.class, groupType));
                }

                @Test
                void nestedMapValueWithGroupMatching() {
                    GroupType groupChild = new GroupType(OPTIONAL, ELEMENT, fieldId, fieldAge, fieldActive);
                    GroupType innerMapType = Types.map(OPTIONAL).key(mapKey).value(groupChild).named(MAP_VALUE);
                    GroupType firstMapType = Types.map(OPTIONAL).key(mapKey).value(innerMapType).named("values");
                    GroupType groupType = new MessageType("foo", fieldName, firstMapType);
                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

                    record Child(String id, int age) {
                    }
                    record NestedMaps(String name, Map<String, Map<String, Child>> values) {
                    }

                    GroupType expectedGroupChild = new GroupType(OPTIONAL, ELEMENT, fieldId, fieldAge);
                    GroupType expectedInnerMapType = Types.map(OPTIONAL).key(mapKey).value(expectedGroupChild)
                            .named(MAP_VALUE);
                    GroupType expectedFirstMapType = Types.map(OPTIONAL).key(mapKey).value(expectedInnerMapType)
                            .named("values");
                    GroupType expectedGroupType = new MessageType("foo", fieldName, expectedFirstMapType);
                    assertEquals(expectedGroupType, filter.project(NestedMaps.class, groupType));
                }

                @Test
                void nestedCollectionValueWithPrimitivesAreSupported() {
                    Type value = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, ELEMENT);
                    GroupType innerListType = listOfElements(REPEATED, MAP_VALUE, value);
                    Type firstMapType = Types.map(OPTIONAL).key(mapKey).value(innerListType).named("values");
                    GroupType groupType = new MessageType("foo", fieldName, firstMapType);

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

                    record NestedList(String name, Map<String, List<Integer>> values) {
                    }
                    assertEquals(groupType, filter.project(NestedList.class, groupType));

                    record Nested(List<Integer> nested) {
                    }
                    record InvalidNestedComposite(String name, Map<String, Nested> list) {
                    }
                    assertThrows(CarpetMissingColumnException.class,
                            () -> filter.project(InvalidNestedComposite.class, groupType));
                }

                @Test
                void nestedCollectionValueWithGroupAreSupported() {
                    GroupType value = new GroupType(OPTIONAL, ELEMENT, fieldId, fieldAge);
                    GroupType innerListType = listOfElements(REPEATED, MAP_VALUE, value);
                    Type firstMapType = Types.map(OPTIONAL).key(mapKey).value(innerListType).named("values");
                    GroupType groupType = new MessageType("foo", fieldName, firstMapType);

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

                    record Child(String id, int age) {
                    }
                    record NestedList(String name, Map<String, List<Child>> values) {
                    }
                    assertEquals(groupType, filter.project(NestedList.class, groupType));

                    record Nested(Map<String, List<Child>> nested) {
                    }
                    record InvalidNestedComposite(String name, Map<String, Nested> list) {
                    }
                    assertThrows(CarpetMissingColumnException.class,
                            () -> filter.project(InvalidNestedComposite.class, groupType));
                }

                @Test
                void nestedCollectionValueWithGroupMatching() {
                    GroupType value = new GroupType(OPTIONAL, ELEMENT, fieldId, fieldAge, fieldActive);
                    GroupType innerListType = listOfElements(REPEATED, MAP_VALUE, value);
                    Type firstMapType = Types.map(OPTIONAL).key(mapKey).value(innerListType).named("values");
                    GroupType groupType = new MessageType("foo", fieldName, firstMapType);

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

                    record Child(String id, int age) {
                    }
                    record NestedList(String name, Map<String, List<Child>> values) {
                    }

                    GroupType expectedValue = new GroupType(OPTIONAL, ELEMENT, fieldId, fieldAge);
                    GroupType expectedInnerListType = listOfElements(REPEATED, MAP_VALUE, expectedValue);
                    Type expectedFirstMapType = Types.map(OPTIONAL).key(mapKey).value(expectedInnerListType)
                            .named("values");
                    GroupType expectedGroupType = new MessageType("foo", fieldName, expectedFirstMapType);
                    assertEquals(expectedGroupType, filter.project(NestedList.class, groupType));
                }

                @Test
                void mapCanBeFiltered() {
                    Type value = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, MAP_VALUE);
                    Type mapType = Types.map(OPTIONAL).key(mapKey).value(value).named("ids");
                    GroupType groupType = new MessageType("foo", fieldName, fieldActive, mapType);

                    record MapFiltered(String name, boolean active) {
                    }

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

                    GroupType expected = new MessageType("foo", fieldName, fieldActive);
                    assertEquals(expected, filter.project(MapFiltered.class, groupType));
                }
            }

            @Nested
            class OtherKeysTypes {

                Type value = Types.primitive(BINARY, OPTIONAL).as(stringType()).named(MAP_VALUE);

                @Test
                void integerKey() {
                    Type key = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, MAP_KEY);
                    Type mapType = Types.map(OPTIONAL).key(key).value(value).named("ids");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapKeyPrimitive(String name, Map<Integer, String> ids) {
                    }

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                    assertEquals(groupType, filter.project(MapKeyPrimitive.class, groupType));
                }

                @Test
                void shortKey() {
                    Type key = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, MAP_KEY);
                    Type mapType = Types.map(OPTIONAL).key(key).value(value).named("ids");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapKeyPrimitive(String name, Map<Short, String> ids) {
                    }

                    SchemaFilter filter = new SchemaFilter(strictNumericConfig, defaultFieldMapper);
                    assertThrows(RecordTypeConversionException.class,
                            () -> filter.project(MapKeyPrimitive.class, groupType));

                    SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                    assertEquals(groupType, filterNonStrict.project(MapKeyPrimitive.class, groupType));
                }

                @Test
                void byteKey() {
                    Type key = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, MAP_KEY);
                    Type mapType = Types.map(OPTIONAL).key(key).value(value).named("ids");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapKeyPrimitive(String name, Map<Byte, String> ids) {
                    }

                    SchemaFilter filter = new SchemaFilter(strictNumericConfig, defaultFieldMapper);
                    assertThrows(RecordTypeConversionException.class,
                            () -> filter.project(MapKeyPrimitive.class, groupType));

                    SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                    assertEquals(groupType, filterNonStrict.project(MapKeyPrimitive.class, groupType));
                }

                @Test
                void longKey() {
                    Type key = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT64, MAP_KEY);
                    Type mapType = Types.map(OPTIONAL).key(key).value(value).named("ids");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapKeyPrimitive(String name, Map<Long, String> ids) {
                    }

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                    assertEquals(groupType, filter.project(MapKeyPrimitive.class, groupType));
                }

                @Test
                void floatKey() {
                    Type key = new PrimitiveType(REQUIRED, PrimitiveTypeName.FLOAT, MAP_KEY);
                    Type mapType = Types.map(OPTIONAL).key(key).value(value).named("ids");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapKeyPrimitive(String name, Map<Float, String> ids) {
                    }

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                    assertEquals(groupType, filter.project(MapKeyPrimitive.class, groupType));
                }

                @Test
                void floatFromDoubleKey() {
                    Type key = new PrimitiveType(REQUIRED, PrimitiveTypeName.DOUBLE, MAP_KEY);
                    Type mapType = Types.map(OPTIONAL).key(key).value(value).named("ids");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapKeyPrimitive(String name, Map<Float, String> ids) {
                    }

                    SchemaFilter filter = new SchemaFilter(strictNumericConfig, defaultFieldMapper);
                    assertThrows(RecordTypeConversionException.class,
                            () -> filter.project(MapKeyPrimitive.class, groupType));

                    SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                    assertEquals(groupType, filterNonStrict.project(MapKeyPrimitive.class, groupType));
                }

                @Test
                void doubleKey() {
                    Type key = new PrimitiveType(REQUIRED, PrimitiveTypeName.DOUBLE, MAP_KEY);
                    Type mapType = Types.map(OPTIONAL).key(key).value(value).named("ids");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapKeyPrimitive(String name, Map<Double, String> ids) {
                    }

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                    assertEquals(groupType, filter.project(MapKeyPrimitive.class, groupType));
                }

                enum Category {
                    one, two
                }

                @Test
                void enumKey() {
                    Type key = Types.primitive(BINARY, REQUIRED).as(enumType()).named(MAP_KEY);
                    Type mapType = Types.map(OPTIONAL).key(key).value(value).named("ids");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapKeyPrimitive(String name, Map<Category, String> ids) {
                    }

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                    assertEquals(groupType, filter.project(MapKeyPrimitive.class, groupType));
                }

                @Test
                void enumToStringKey() {
                    Type key = Types.primitive(BINARY, REQUIRED).as(enumType()).named(MAP_KEY);
                    Type mapType = Types.map(OPTIONAL).key(key).value(value).named("ids");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapKeyPrimitive(String name, Map<String, String> ids) {
                    }

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                    assertEquals(groupType, filter.project(MapKeyPrimitive.class, groupType));
                }

                @Test
                void compositeKey() {
                    Type key = new GroupType(REQUIRED, MAP_KEY, fieldId, fieldAge);
                    Type mapType = Types.map(OPTIONAL).key(key).value(value).named("child");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record Child(String id, int age) {
                    }
                    record MapValueComposite(String name, Map<Child, String> child) {
                    }

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
                    assertEquals(groupType, filter.project(MapValueComposite.class, groupType));
                }

                @Test
                void compositeValueMatchFields() {
                    // Supported, but bad practice remove elements from Key map
                    Type key = new GroupType(REQUIRED, MAP_KEY, fieldId, fieldAge, fieldActive);
                    Type mapType = Types.map(OPTIONAL).key(key).value(value).named("child");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record Child(String id, int age) {
                    }
                    record MapValueComposite(String name, Map<Child, String> child) {
                    }

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

                    Type expectedKey = new GroupType(REQUIRED, MAP_KEY, fieldId, fieldAge);
                    Type expectedMapType = Types.map(OPTIONAL).key(expectedKey).value(value).named("child");
                    GroupType groupTypeExpected = new MessageType("foo", fieldName, expectedMapType);
                    assertEquals(groupTypeExpected, filter.project(MapValueComposite.class, groupType));
                }

                @Test
                void nestedMapsKeyAreNotSupported() {
                    Type nestedValueId = Types.primitive(BINARY, REQUIRED).as(stringType()).named(MAP_KEY);
                    Type innerMapKeyType = Types.map(REQUIRED).key(nestedValueId).value(value).named(MAP_KEY);
                    Type count = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, MAP_VALUE);
                    Type firstMapType = Types.map(OPTIONAL).key(innerMapKeyType).value(count).named("values");
                    GroupType groupType = new MessageType("foo", fieldName, firstMapType);

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

                    record NestedMaps(String name, Map<Map<String, Integer>, Integer> values) {
                    }
                    assertThrows(RecordTypeConversionException.class,
                            () -> filter.project(NestedMaps.class, groupType));
                }

                @Test
                void nestedCollectionsKeyAreNotSupported() {
                    Type nestedValueId = Types.primitive(BINARY, REQUIRED).as(stringType()).named(ELEMENT);
                    Type innerListKeyType = listOfElements(REQUIRED, MAP_KEY, nestedValueId);
                    Type count = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, MAP_VALUE);
                    Type firstMapType = Types.map(OPTIONAL).key(innerListKeyType).value(count).named("values");
                    GroupType groupType = new MessageType("foo", fieldName, firstMapType);

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

                    record NestedMaps(String name, Map<List<String>, Integer> values) {
                    }
                    assertThrows(RecordTypeConversionException.class,
                            () -> filter.project(NestedMaps.class, groupType));
                }
            }
        }

    }

    @Nested
    class MissingFields {

        @Test
        void parquetColumnsCanBeSkipped() {
            Type fieldName = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type fieldId = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            Type fieldAge = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType groupType = new MessageType("foo", fieldName, fieldId, fieldAge);

            record Main(String id, String name) {
            }

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertEquals(new MessageType("foo", fieldName, fieldId), filter.project(Main.class, groupType));
        }

        @Test
        void ifFieldIsMissingValidationFails() {
            Type fieldName = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type fieldId = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            Type fieldAge = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType groupType = new MessageType("foo", fieldName, fieldId, fieldAge);

            record Main(String id, String name, int age, String missingField) {
            }

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertThrows(CarpetMissingColumnException.class, () -> filter.project(Main.class, groupType));
        }

        @Test
        void ifFieldIsMissingValidationDoesntFailIfIgnoreUnknown() {
            Type fieldName = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type fieldId = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            Type fieldAge = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType groupType = new MessageType("foo", fieldName, fieldId, fieldAge);

            record Main(String id, String name, int age, String missingField) {
            }

            SchemaFilter filter = new SchemaFilter(dontFailOnMissingFields, defaultFieldMapper);
            assertEquals(groupType, filter.project(Main.class, groupType));
        }

        @Test
        void ifFieldInCompositeIsMissingValidationFails() {
            Type fieldCode = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("code");
            Type fieldSize = new PrimitiveType(REQUIRED, PrimitiveTypeName.DOUBLE, "size");
            GroupType childGroupType = new GroupType(OPTIONAL, "child", fieldCode, fieldSize);

            Type fieldName = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type fieldId = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            Type fieldAge = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType groupType = new MessageType("foo", fieldName, fieldId, fieldAge, childGroupType);

            record Child(String code, double size, int missingField) {
            }

            record CompositeMain(String id, String name, int age, Child child) {
            }

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertThrows(CarpetMissingColumnException.class, () -> filter.project(CompositeMain.class, groupType));
        }

        @Test
        void ifFieldInCompositeIsMissingValidationDoesntFailIfIgnoreUnknown() {
            Type fieldCode = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("code");
            Type fieldSize = new PrimitiveType(REQUIRED, PrimitiveTypeName.DOUBLE, "size");
            GroupType childGroupType = new GroupType(OPTIONAL, "child", fieldCode, fieldSize);

            Type fieldName = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type fieldId = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            Type fieldAge = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType groupType = new MessageType("foo", fieldName, fieldId, fieldAge, childGroupType);

            record Child(String code, double size, int missingField) {
            }

            record CompositeMain(String id, String name, int age, Child child) {
            }

            SchemaFilter filter = new SchemaFilter(dontFailOnMissingFields, defaultFieldMapper);
            assertEquals(groupType, filter.project(CompositeMain.class, groupType));
        }

        @Test
        void ifFieldInCompositeListIsMissingValidationFails() {
            Type fieldCode = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("code");
            Type fieldSize = new PrimitiveType(REQUIRED, PrimitiveTypeName.DOUBLE, "size");
            GroupType childGroupType = new GroupType(OPTIONAL, "element", fieldCode, fieldSize);

            Type fieldName = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type fieldId = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            Type fieldAge = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType listType = listOfElements(OPTIONAL, "children", childGroupType);
            GroupType groupType = new MessageType("foo", fieldName, fieldId, fieldAge, listType);

            record Child(String code, double size, int missingField) {
            }

            record CompositeMain(String id, String name, int age, List<Child> children) {
            }

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertThrows(CarpetMissingColumnException.class, () -> filter.project(CompositeMain.class, groupType));
        }
    }

    @Nested
    class IncompatibleTypes {

        @Test
        void numericIsIncompatibleWithString() {
            Type fieldName = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type fieldId = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            Type fieldAge = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType groupType = new MessageType("foo", fieldName, fieldId, fieldAge);

            record Main(String id, String name, String age) {
            }

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertThrows(RecordTypeConversionException.class, () -> filter.project(Main.class, groupType));
        }

        @Test
        void stringIsIncompatibleWithNumeric() {
            Type fieldName = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type fieldId = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            Type fieldAge = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType groupType = new MessageType("foo", fieldName, fieldId, fieldAge);

            record Main(String id, int name, int age) {
            }

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertThrows(RecordTypeConversionException.class, () -> filter.project(Main.class, groupType));
        }

        @Test
        void primitiveIsIncompatibleWithList() {
            Type fieldName = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type fieldId = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            Type fieldAge = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType groupType = new MessageType("foo", fieldName, fieldId, fieldAge);

            record Main(String id, String name, List<Integer> age) {
            }

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertThrows(RecordTypeConversionException.class, () -> filter.project(Main.class, groupType));
        }

        @Test
        void oneLevelsListIsIncompatibleWithPrimitive() {
            Type fieldName = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type fieldId = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            Type fieldAge = new PrimitiveType(REPEATED, PrimitiveTypeName.INT32, "ages");
            GroupType groupType = new MessageType("foo", fieldName, fieldId, fieldAge);

            record Main(String id, String name, Integer ages) {
            }

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertThrows(RecordTypeConversionException.class, () -> filter.project(Main.class, groupType));
        }

        @Test
        void twoLevelsListIsIncompatibleWithPrimitive() {
            Type fieldName = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type fieldId = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            Type fieldAge = new PrimitiveType(REPEATED, PrimitiveTypeName.INT32, "element");
            GroupType listType = ConversionPatterns.listType(OPTIONAL, "ages", fieldAge);
            GroupType groupType = new MessageType("foo", fieldName, fieldId, listType);

            record Main(String id, String name, Integer ages) {
            }

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertThrows(RecordTypeConversionException.class, () -> filter.project(Main.class, groupType));
        }

        @Test
        void threeLevelsListIsIncompatibleWithPrimitive() {
            Type fieldName = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type fieldId = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            Type fieldAge = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "element");
            GroupType listType = listOfElements(OPTIONAL, "ages", fieldAge);
            GroupType groupType = new MessageType("foo", fieldName, fieldId, listType);

            record Main(String id, String name, Integer ages) {
            }

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertThrows(RecordTypeConversionException.class, () -> filter.project(Main.class, groupType));
        }

        @Test
        void primitiveIsIncompatibleWithMap() {
            Type fieldName = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type fieldId = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            Type fieldCode = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("code");
            GroupType groupType = new MessageType("foo", fieldName, fieldId, fieldCode);

            record Main(String id, String name, Map<Integer, String> code) {
            }

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertThrows(RecordTypeConversionException.class, () -> filter.project(Main.class, groupType));
        }

        @Test
        void mapIsIncompatibleWithPrimitive() {
            Type fieldName = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type fieldId = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            Type key = Types.primitive(BINARY, REQUIRED).as(stringType()).named(MAP_KEY);
            Type value = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, MAP_VALUE);
            Type fieldCode = Types.map(OPTIONAL).key(key).value(value).named("code");
            GroupType groupType = new MessageType("foo", fieldName, fieldId, fieldCode);

            record Main(String id, String name, Integer code) {
            }

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertThrows(RecordTypeConversionException.class, () -> filter.project(Main.class, groupType));
        }

    }

    @Nested
    class GroupToMapConversion {

        @Test
        void rootAsMap() {
            Type field1 = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type childField1 = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            Type childField2 = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType childGroupType = new GroupType(OPTIONAL, "child", childField1, childField2);
            GroupType groupType = new MessageType("foo", field1, childGroupType);

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertEquals(groupType, filter.project(Map.class, groupType));
        }

        @Test
        void compositeChild() {
            Type field1 = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type childField1 = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            Type childField2 = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType childGroupType = new GroupType(OPTIONAL, "child", childField1, childField2);
            GroupType groupType = new MessageType("foo", field1, childGroupType);

            record CompositeMain(String name, Map<String, Object> child) {
            }

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);
            assertEquals(groupType, filter.project(CompositeMain.class, groupType));
        }

    }

    @Nested
    class SnakeCaseConversion {

        private final ColumnToFieldMapper noSnakeCaseMap = new ColumnToFieldMapper(FIELD_NAME);
        private final ColumnToFieldMapper snakeCaseMap = new ColumnToFieldMapper(SNAKE_CASE);

        @Test
        void canNotMapSnakeCaseColumnNameIfDisabled() {

            Type field = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "some_value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, noSnakeCaseMap);

            record CamelCaseField(int someValue) {
            }

            assertThrows(CarpetMissingColumnException.class, () -> filter.project(CamelCaseField.class, groupType));
        }

        @Test
        void mapSnakeCaseColumnNameWhenEnabled() {

            Type field = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "some_value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, snakeCaseMap);

            record CamelCaseField(int someValue) {
            }

            assertEquals(groupType, filter.project(CamelCaseField.class, groupType));
        }

        @Test
        void priorityOnMatchingColumns() {

            Type field1 = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "some_value");
            Type field2 = new PrimitiveType(REQUIRED, PrimitiveTypeName.DOUBLE, "someValue");
            GroupType groupType = new MessageType("foo", field1, field2);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

            record MixedValues(int some_value, double someValue) {
            }

            assertEquals(groupType, filter.project(MixedValues.class, groupType));
        }

        @Test
        void priorityOnAliasedFields() {

            Type field1 = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "some_value");
            Type field2 = new PrimitiveType(REQUIRED, PrimitiveTypeName.DOUBLE, "someValue");
            GroupType groupType = new MessageType("foo", field1, field2);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, defaultFieldMapper);

            record MixedValues(@Alias("some_value") int foo, @Alias("someValue") double bar) {
            }

            assertEquals(groupType, filter.project(MixedValues.class, groupType));
        }

    }
}
