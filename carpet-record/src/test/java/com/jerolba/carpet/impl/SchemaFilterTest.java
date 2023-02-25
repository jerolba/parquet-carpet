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
package com.jerolba.carpet.impl;

import static org.apache.parquet.schema.ConversionPatterns.listOfElements;
import static org.apache.parquet.schema.ConversionPatterns.listType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.jerolba.carpet.CarpetMissingColumnException;
import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.annotation.NotNull;
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

    @Nested
    class FieldInt32Conversion {

        @Test
        void fieldRequired() {
            Type field = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            record PrimitiveInteger(int value) {
            }
            assertEquals(groupType, filter.project(PrimitiveInteger.class));

            record ObjectInteger(Integer value) {
            }
            assertEquals(groupType, filter.project(ObjectInteger.class));
        }

        @Test
        void fieldOptional() {
            Type field = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filterDefault = new SchemaFilter(defaultReadConfig, groupType);
            SchemaFilter filterFailOnNotNullable = new SchemaFilter(failOnNullForPrimitives, groupType);

            record PrimitiveInteger(int value) {
            }
            assertEquals(groupType, filterDefault.project(PrimitiveInteger.class));
            assertThrows(RecordTypeConversionException.class,
                    () -> filterFailOnNotNullable.project(PrimitiveInteger.class));

            record ObjectInteger(Integer value) {
            }
            assertEquals(groupType, filterDefault.project(ObjectInteger.class));
            assertEquals(groupType, filterFailOnNotNullable.project(ObjectInteger.class));
        }

        @Test
        void castToLongIsSupported() {
            Type field = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            record PrimitiveLong(long value) {
            }
            assertEquals(groupType, filter.project(PrimitiveLong.class));

            record ObjectLong(Long value) {
            }
            assertEquals(groupType, filter.project(ObjectLong.class));
        }

        @Test
        void castToShortIsSupportedIfStrictNotActive() {
            Type field = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "value");
            GroupType groupType = new MessageType("foo", field);

            record PrimitiveShort(short value) {
            }
            record ObjectShort(Short value) {
            }

            SchemaFilter filterStrict = new SchemaFilter(strictNumericConfig, groupType);
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.project(PrimitiveShort.class));
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.project(ObjectShort.class));

            SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, groupType);
            assertEquals(groupType, filterNonStrict.project(PrimitiveShort.class));
            assertEquals(groupType, filterNonStrict.project(ObjectShort.class));
        }

        @Test
        void castToByteIsSupportedIfStrictNotActive() {
            Type field = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "value");
            GroupType groupType = new MessageType("foo", field);

            record PrimitiveByte(byte value) {
            }
            record ObjectByte(Byte value) {
            }

            SchemaFilter filterStrict = new SchemaFilter(strictNumericConfig, groupType);
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.project(PrimitiveByte.class));
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.project(ObjectByte.class));

            SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, groupType);
            assertEquals(groupType, filterNonStrict.project(PrimitiveByte.class));
            assertEquals(groupType, filterNonStrict.project(ObjectByte.class));
        }

    }

    @Nested
    class FieldInt64Conversion {

        @Test
        void fieldRequired() {
            Type field = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT64, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            record PrimitiveLong(long value) {
            }
            assertEquals(groupType, filter.project(PrimitiveLong.class));

            record ObjectLong(Long value) {
            }
            assertEquals(groupType, filter.project(ObjectLong.class));
        }

        @Test
        void fieldOptional() {
            Type field = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT64, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filterDefault = new SchemaFilter(defaultReadConfig, groupType);
            SchemaFilter filterFailOnNotNullable = new SchemaFilter(failOnNullForPrimitives, groupType);

            record PrimitiveLong(long value) {
            }
            assertEquals(groupType, filterDefault.project(PrimitiveLong.class));
            assertThrows(RecordTypeConversionException.class,
                    () -> filterFailOnNotNullable.project(PrimitiveLong.class));

            record ObjectLong(Long value) {
            }
            assertEquals(groupType, filterDefault.project(PrimitiveLong.class));
            assertEquals(groupType, filterFailOnNotNullable.project(ObjectLong.class));
        }

        @Test
        void castToIntIsSupportedIfStrictNotActive() {
            Type field = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT64, "value");
            GroupType groupType = new MessageType("foo", field);

            record PrimitiveInt(int value) {
            }
            record ObjectInteger(Integer value) {
            }

            SchemaFilter filterStrict = new SchemaFilter(strictNumericConfig, groupType);
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.project(PrimitiveInt.class));
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.project(ObjectInteger.class));

            SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, groupType);
            assertEquals(groupType, filterNonStrict.project(PrimitiveInt.class));
            assertEquals(groupType, filterNonStrict.project(ObjectInteger.class));
        }

        @Test
        void castToShortIsSupportedIfStrictNotActive() {
            Type field = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT64, "value");
            GroupType groupType = new MessageType("foo", field);

            record PrimitiveShort(short value) {
            }
            record ObjectShort(Short value) {
            }

            SchemaFilter filterStrict = new SchemaFilter(strictNumericConfig, groupType);
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.project(PrimitiveShort.class));
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.project(ObjectShort.class));

            SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, groupType);
            assertEquals(groupType, filterNonStrict.project(PrimitiveShort.class));
            assertEquals(groupType, filterNonStrict.project(ObjectShort.class));
        }

        @Test
        void castToByteIsSupportedIfStrictNotActive() {
            Type field = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT64, "value");
            GroupType groupType = new MessageType("foo", field);

            record PrimitiveByte(byte value) {
            }
            record ObjectByte(Byte value) {
            }

            SchemaFilter filterStrict = new SchemaFilter(strictNumericConfig, groupType);
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.project(PrimitiveByte.class));
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.project(ObjectByte.class));

            SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, groupType);
            assertEquals(groupType, filterNonStrict.project(PrimitiveByte.class));
            assertEquals(groupType, filterNonStrict.project(ObjectByte.class));
        }

    }

    @Nested
    class FieldFloatConversion {

        @Test
        void fieldRequired() {
            Type field = new PrimitiveType(REQUIRED, PrimitiveTypeName.FLOAT, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            record PrimitiveFloat(float value) {
            }
            assertEquals(groupType, filter.project(PrimitiveFloat.class));

            record ObjectFloat(Float value) {
            }
            assertEquals(groupType, filter.project(ObjectFloat.class));
        }

        @Test
        void fieldOptional() {
            Type field = new PrimitiveType(OPTIONAL, PrimitiveTypeName.FLOAT, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filterDefault = new SchemaFilter(defaultReadConfig, groupType);
            SchemaFilter filterFailOnNotNullable = new SchemaFilter(failOnNullForPrimitives, groupType);

            record PrimitiveFloat(float value) {
            }
            assertEquals(groupType, filterDefault.project(PrimitiveFloat.class));
            assertThrows(RecordTypeConversionException.class,
                    () -> filterFailOnNotNullable.project(PrimitiveFloat.class));

            record ObjectFloat(Float value) {
            }
            assertEquals(groupType, filterDefault.project(ObjectFloat.class));
            assertEquals(groupType, filterFailOnNotNullable.project(ObjectFloat.class));
        }

        @Test
        void castToDoubleIsSupported() {
            Type field = new PrimitiveType(REQUIRED, PrimitiveTypeName.FLOAT, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            record PrimitiveDouble(double value) {
            }
            assertEquals(groupType, filter.project(PrimitiveDouble.class));

            record ObjectDouble(Double value) {
            }
            assertEquals(groupType, filter.project(ObjectDouble.class));
        }

    }

    @Nested
    class FieldDoubleConversion {

        @Test
        void fieldRequired() {
            Type field = new PrimitiveType(REQUIRED, PrimitiveTypeName.DOUBLE, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            record PrimitiveDouble(double value) {
            }
            assertEquals(groupType, filter.project(PrimitiveDouble.class));

            record ObjectDouble(Double value) {
            }
            assertEquals(groupType, filter.project(ObjectDouble.class));
        }

        @Test
        void fieldOptional() {
            Type field = new PrimitiveType(OPTIONAL, PrimitiveTypeName.DOUBLE, "value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filterDefault = new SchemaFilter(defaultReadConfig, groupType);
            SchemaFilter filterFailOnNotNullable = new SchemaFilter(failOnNullForPrimitives, groupType);

            record PrimitiveDouble(double value) {
            }
            assertEquals(groupType, filterDefault.project(PrimitiveDouble.class));
            assertThrows(RecordTypeConversionException.class,
                    () -> filterFailOnNotNullable.project(PrimitiveDouble.class));

            record ObjectDouble(Double value) {
            }
            assertEquals(groupType, filterDefault.project(ObjectDouble.class));
            assertEquals(groupType, filterFailOnNotNullable.project(ObjectDouble.class));
        }

        @Test
        void castToFloatIsSupportedIfStrictNotActive() {
            Type field = new PrimitiveType(REQUIRED, PrimitiveTypeName.DOUBLE, "value");
            GroupType groupType = new MessageType("foo", field);

            record PrimitiveFloat(float value) {
            }
            record ObjectFloat(Float value) {
            }

            SchemaFilter filterStrict = new SchemaFilter(strictNumericConfig, groupType);
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.project(PrimitiveFloat.class));
            assertThrows(RecordTypeConversionException.class, () -> filterStrict.project(ObjectFloat.class));

            SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, groupType);
            assertEquals(groupType, filterNonStrict.project(PrimitiveFloat.class));
            assertEquals(groupType, filterNonStrict.project(ObjectFloat.class));
        }

    }

    @Nested
    class FieldStringConversion {

        @Test
        void fieldRequired() {
            Type field = Types.primitive(BINARY, REQUIRED).as(stringType()).named("value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            record NotNullString(@NotNull String value) {
            }
            assertEquals(groupType, filter.project(NotNullString.class));

            record NullableString(String value) {
            }
            assertEquals(groupType, filter.project(NullableString.class));
        }

        @Test
        void fieldOptional() {
            Type field = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filterDefault = new SchemaFilter(defaultReadConfig, groupType);
            SchemaFilter filterFailOnNotNullable = new SchemaFilter(failOnNullForPrimitives, groupType);

            record NotNullString(@NotNull String value) {
            }
            assertEquals(groupType, filterDefault.project(NotNullString.class));
            assertThrows(RecordTypeConversionException.class,
                    () -> filterFailOnNotNullable.project(NotNullString.class));

            record NullableString(String value) {
            }
            assertEquals(groupType, filterDefault.project(NullableString.class));
            assertEquals(groupType, filterFailOnNotNullable.project(NullableString.class));
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
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            record NotNullEnum(@NotNull Category value) {
            }
            assertEquals(groupType, filter.project(NotNullEnum.class));

            record NullableEnum(Category value) {
            }
            assertEquals(groupType, filter.project(NullableEnum.class));
        }

        @Test
        void fieldOptional() {
            Type field = Types.primitive(BINARY, OPTIONAL).as(enumType()).named("value");
            GroupType groupType = new MessageType("foo", field);
            SchemaFilter filterDefault = new SchemaFilter(defaultReadConfig, groupType);
            SchemaFilter filterFailOnNotNullable = new SchemaFilter(failOnNullForPrimitives, groupType);

            record NotNullEnum(@NotNull Category value) {
            }
            assertEquals(groupType, filterDefault.project(NotNullEnum.class));
            assertThrows(RecordTypeConversionException.class, () -> filterFailOnNotNullable.project(NotNullEnum.class));

            record NullableEnum(Category value) {
            }
            assertEquals(groupType, filterDefault.project(NullableEnum.class));
            assertEquals(groupType, filterFailOnNotNullable.project(NullableEnum.class));
        }

        @Test
        void castToStringIsSupported() {
            Type field = Types.primitive(BINARY, REQUIRED).as(enumType()).named("value");
            GroupType groupType = new MessageType("foo", field);

            record CastToString(String value) {
            }

            SchemaFilter filterStrict = new SchemaFilter(defaultReadConfig, groupType);
            assertEquals(groupType, filterStrict.project(CastToString.class));
        }

    }

    @Nested
    class ParquetFieldsMatchingRecordFields {

        @Test
        void fieldsMatch() {
            Type field1 = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type field2 = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType groupType = new MessageType("foo", field1, field2);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            record AllPresent(String name, int age) {
            }
            assertEquals(groupType, filter.project(AllPresent.class));
        }

        @Test
        void ifSchemaHasMoreFieldsThanNeededAreFilteredInProjection() {
            Type field1 = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type field2 = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType groupType = new MessageType("foo", field1, field2);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            record OnlyName(String name) {
            }
            GroupType expectedName = new MessageType("foo", field1);
            assertEquals(expectedName, filter.project(OnlyName.class));

            record OnlyAge(int age) {
            }
            GroupType expectedAge = new MessageType("foo", field2);
            assertEquals(expectedAge, filter.project(OnlyAge.class));
        }

        @Test
        void ifSchemaHasLessFieldsThanNeededProjectionFails() {
            Type field1 = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type field2 = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType groupType = new MessageType("foo", field1, field2);
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            record MoreThanExisting(String name, int age, boolean active) {
            }

            assertThrows(CarpetMissingColumnException.class, () -> filter.project(MoreThanExisting.class));
        }

        @Test
        void ifSchemaHasLessFieldsThanNeededButSupportsItFieldsAreNulled() {
            Type field1 = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type field2 = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType groupType = new MessageType("foo", field1, field2);

            record MoreThanExisting(String name, int age, boolean active) {
            }

            SchemaFilter filter = new SchemaFilter(dontFailOnMissingFields, groupType);
            GroupType expectedAge = new MessageType("foo", field1, field2);
            assertEquals(expectedAge, filter.project(MoreThanExisting.class));
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

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
            assertEquals(groupType, filter.project(CompositeMain.class));
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

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

            GroupType expectedChildGroupType = new GroupType(OPTIONAL, "child", childField1, childField2);
            GroupType expectedGroup = new MessageType("foo", field1, expectedChildGroupType);
            assertEquals(expectedGroup, filter.project(CompositeMain.class));
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

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
            assertThrows(CarpetMissingColumnException.class, () -> filter.project(CompositeMain.class));
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

            SchemaFilter filter = new SchemaFilter(dontFailOnMissingFields, groupType);

            assertEquals(groupType, filter.project(CompositeMain.class));
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

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
            assertThrows(RecordTypeConversionException.class, () -> filter.project(NormalClass.class));
        }

        @Test
        void javaBeanCanNotBePartOfRecord() {
            Type field1 = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            Type childField1 = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            GroupType childGroupType = new GroupType(OPTIONAL, "child", childField1);
            GroupType groupType = new MessageType("foo", field1, childGroupType);

            record ParentClass(String id, NormalClass child) {
            }

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
            assertThrows(RecordTypeConversionException.class, () -> filter.project(ParentClass.class));

            record ChildRecord(String id) {
            }
            record ParentRecord(String id, ChildRecord child) {
            }
            assertEquals(groupType, filter.project(ParentRecord.class));
        }

        @Test
        void bigIntegerIsNotSupported() {
            Type field1 = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT64, "id");
            GroupType groupType = new MessageType("foo", field1);

            record BigIntegerRecord(BigInteger id) {
            }
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
            assertThrows(RecordTypeConversionException.class, () -> filter.project(BigIntegerRecord.class));
        }

        @Test
        void bigDecimalIsNotSupported() {
            Type field1 = new PrimitiveType(REQUIRED, PrimitiveTypeName.DOUBLE, "value");
            GroupType groupType = new MessageType("foo", field1);

            record BigDecimalRecord(BigDecimal value) {
            }
            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
            assertThrows(RecordTypeConversionException.class, () -> filter.project(BigDecimalRecord.class));
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

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.project(LevelOnePrimitive.class));
            }

            @Test
            void shortCollection() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.INT32, "ids");
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record LevelOnePrimitive(String name, List<Short> ids) {
                }

                SchemaFilter filter = new SchemaFilter(strictNumericConfig, groupType);
                assertThrows(RecordTypeConversionException.class, () -> filter.project(LevelOnePrimitive.class));

                SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filterNonStrict.project(LevelOnePrimitive.class));
            }

            @Test
            void byteCollection() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.INT32, "ids");
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record LevelOnePrimitive(String name, List<Byte> ids) {
                }

                SchemaFilter filter = new SchemaFilter(strictNumericConfig, groupType);
                assertThrows(RecordTypeConversionException.class, () -> filter.project(LevelOnePrimitive.class));

                SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filterNonStrict.project(LevelOnePrimitive.class));
            }

            @Test
            void longCollection() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.INT64, "ids");
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record LevelOnePrimitive(String name, List<Long> ids) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.project(LevelOnePrimitive.class));
            }

            @Test
            void floatCollection() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.FLOAT, "values");
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record LevelOnePrimitive(String name, List<Float> values) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.project(LevelOnePrimitive.class));
            }

            @Test
            void floatFromDoubleCollection() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.DOUBLE, "values");
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record LevelOnePrimitive(String name, List<Float> values) {
                }

                SchemaFilter filter = new SchemaFilter(strictNumericConfig, groupType);
                assertThrows(RecordTypeConversionException.class, () -> filter.project(LevelOnePrimitive.class));

                SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filterNonStrict.project(LevelOnePrimitive.class));
            }

            @Test
            void doubleCollection() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.DOUBLE, "values");
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record LevelOnePrimitive(String name, List<Double> values) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.project(LevelOnePrimitive.class));
            }

            @Test
            void stringCollection() {
                Type repeated = Types.primitive(BINARY, REPEATED).as(stringType()).named("ids");
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record LevelOnePrimitive(String name, List<String> ids) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.project(LevelOnePrimitive.class));
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

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.project(LevelOnePrimitive.class));
            }

            @Test
            void enumToStringCollection() {
                Type repeated = Types.primitive(BINARY, REPEATED).as(enumType()).named("categories");
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record LevelOnePrimitive(String name, List<String> categories) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.project(LevelOnePrimitive.class));
            }

            @Test
            void compositeCollection() {
                GroupType repeated = new GroupType(REPEATED, "child", fieldId, fieldAge);
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record Child(String id, int age) {
                }
                record LevelOneComposite(String name, List<Child> child) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.project(LevelOneComposite.class));
            }

            @Test
            void compositeCollectionMatchFields() {
                GroupType repeated = new GroupType(REPEATED, "child", fieldId, fieldAge, fieldActive);
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                record Child(String id, int age) {
                }
                record LevelOneComposite(String name, List<Child> child) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

                GroupType repeatedExoected = new GroupType(REPEATED, "child", fieldId, fieldAge);
                GroupType groupTypeExpected = new MessageType("foo", fieldName, repeatedExoected);
                assertEquals(groupTypeExpected, filter.project(LevelOneComposite.class));
            }

            @Test
            void nestedCollectionsAreNotSupported() {
                GroupType repeatedChild = new GroupType(REPEATED, "nested", fieldId, fieldAge);
                GroupType repeated = new GroupType(REPEATED, "list", repeatedChild);
                GroupType groupType = new MessageType("foo", fieldName, repeated);
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

                record Child(String id, int age) {
                }
                record NestedCollections(String name, List<List<Child>> list) {
                }

                assertThrows(RecordTypeConversionException.class, () -> filter.project(NestedCollections.class));

                record Nested(List<Child> nested) {
                }
                record ValidLevelOneComposite(String name, List<Nested> list) {
                }
                assertEquals(groupType, filter.project(ValidLevelOneComposite.class));
            }

            @Test
            void collectionCanBeFiltered() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.INT32, "ids");
                GroupType groupType = new MessageType("foo", fieldName, fieldActive, fieldId, repeated);

                record LevelOnePrimitive(String id, String name, boolean active) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                GroupType expected = new MessageType("foo", fieldName, fieldActive, fieldId);
                assertEquals(expected, filter.project(LevelOnePrimitive.class));
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

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.project(LevelTwoPrimitive.class));
            }

            @Test
            void shortCollection() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.INT32, ELEMENT);
                GroupType listType = listType(OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<Short> ids) {
                }

                SchemaFilter filter = new SchemaFilter(strictNumericConfig, groupType);
                assertThrows(RecordTypeConversionException.class, () -> filter.project(LevelTwoPrimitive.class));

                SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filterNonStrict.project(LevelTwoPrimitive.class));
            }

            @Test
            void byteCollection() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.INT32, ELEMENT);
                GroupType listType = listType(OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<Byte> ids) {
                }

                SchemaFilter filter = new SchemaFilter(strictNumericConfig, groupType);
                assertThrows(RecordTypeConversionException.class, () -> filter.project(LevelTwoPrimitive.class));

                SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filterNonStrict.project(LevelTwoPrimitive.class));
            }

            @Test
            void longCollection() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.INT64, ELEMENT);
                GroupType listType = listType(OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<Long> ids) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.project(LevelTwoPrimitive.class));
            }

            @Test
            void floatCollection() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.FLOAT, ELEMENT);
                GroupType listType = listType(OPTIONAL, "values", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<Float> values) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.project(LevelTwoPrimitive.class));
            }

            @Test
            void floatFromDoubleCollection() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.DOUBLE, ELEMENT);
                GroupType listType = listType(OPTIONAL, "values", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<Float> values) {
                }

                SchemaFilter filter = new SchemaFilter(strictNumericConfig, groupType);
                assertThrows(RecordTypeConversionException.class, () -> filter.project(LevelTwoPrimitive.class));

                SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filterNonStrict.project(LevelTwoPrimitive.class));
            }

            @Test
            void doubleCollection() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.DOUBLE, ELEMENT);
                GroupType listType = listType(OPTIONAL, "values", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<Double> values) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.project(LevelTwoPrimitive.class));
            }

            @Test
            void stringCollection() {
                Type repeated = Types.primitive(BINARY, REPEATED).as(stringType()).named(ELEMENT);
                GroupType listType = listType(OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelOnePrimitive(String name, List<String> ids) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.project(LevelOnePrimitive.class));
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

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.project(LevelOnePrimitive.class));
            }

            @Test
            void enumToStringCollection() {
                Type repeated = Types.primitive(BINARY, REPEATED).as(enumType()).named(ELEMENT);
                GroupType listType = listType(OPTIONAL, "categories", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelTwoPrimitive(String name, List<String> categories) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.project(LevelTwoPrimitive.class));
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

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.project(LevelTwoComposite.class));
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

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

                GroupType repeatedExpected = new GroupType(REPEATED, "child", fieldId, fieldAge);
                GroupType listTypeExpected = listType(OPTIONAL, "child", repeatedExpected);
                GroupType groupTypeExpected = new MessageType("foo", fieldName, listTypeExpected);
                assertEquals(groupTypeExpected, filter.project(LevelTwoComposite.class));
            }

            @Test
            void nestedCollectionsWithPrimitivesAreSupported() {
                Type repeatedChild = new PrimitiveType(REPEATED, PrimitiveTypeName.INT32, ELEMENT);
                GroupType listType = listType(REPEATED, ELEMENT, repeatedChild);
                GroupType repeated = listType(OPTIONAL, "values", listType);
                GroupType groupType = new MessageType("foo", fieldName, repeated);
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

                record NestedCollections(String name, List<List<Integer>> values) {
                }
                assertEquals(groupType, filter.project(NestedCollections.class));

                record Nested(List<Integer> nested) {
                }
                record ValidLevelTwoComposite(String name, List<Nested> list) {
                }
                assertThrows(CarpetMissingColumnException.class, () -> filter.project(ValidLevelTwoComposite.class));
            }

            @Test
            void nestedCollectionsWithGroupAreSupported() {
                GroupType repeatedChild = new GroupType(REPEATED, ELEMENT, fieldId, fieldAge);
                GroupType listType = listType(REPEATED, ELEMENT, repeatedChild);
                GroupType repeated = listType(OPTIONAL, "values", listType);
                GroupType groupType = new MessageType("foo", fieldName, repeated);
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

                record Child(String id, int age) {
                }
                record NestedCollections(String name, List<List<Child>> values) {
                }
                assertEquals(groupType, filter.project(NestedCollections.class));

                record Nested(List<Child> nested) {
                }
                record ValidLevelTwoComposite(String name, List<Nested> list) {
                }
                assertThrows(CarpetMissingColumnException.class, () -> filter.project(ValidLevelTwoComposite.class));
            }

            @Test
            void collectionCanBeFiltered() {
                Type repeated = new PrimitiveType(REPEATED, PrimitiveTypeName.INT32, ELEMENT);
                GroupType listType = listType(OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, fieldActive, fieldId, listType);

                record LevelTwoPrimitive(String id, String name, boolean active) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                GroupType expected = new MessageType("foo", fieldName, fieldActive, fieldId);
                assertEquals(expected, filter.project(LevelTwoPrimitive.class));
            }

            @Test
            void nestedMapsAreSupported() {
                Type key = Types.primitive(BINARY, REQUIRED).as(stringType()).named(MAP_KEY);
                Type value = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, MAP_VALUE);
                Type mapType = Types.map(REPEATED).key(key).value(value).named(ELEMENT);
                GroupType repeated = listType(OPTIONAL, "values", mapType);
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

                record NestedMap(String name, List<Map<String, Integer>> values) {
                }
                assertEquals(groupType, filter.project(NestedMap.class));
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
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.project(LevelThreePrimitive.class));
            }

            @Test
            void shortCollection() {
                Type repeated = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, ELEMENT);
                GroupType listType = listOfElements(OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelThreePrimitive(String name, List<Short> ids) {
                }
                SchemaFilter filter = new SchemaFilter(strictNumericConfig, groupType);
                assertThrows(RecordTypeConversionException.class, () -> filter.project(LevelThreePrimitive.class));

                SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filterNonStrict.project(LevelThreePrimitive.class));
            }

            @Test
            void byteCollection() {
                Type repeated = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, ELEMENT);
                GroupType listType = listOfElements(OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelThreePrimitive(String name, List<Byte> ids) {
                }
                SchemaFilter filter = new SchemaFilter(strictNumericConfig, groupType);
                assertThrows(RecordTypeConversionException.class, () -> filter.project(LevelThreePrimitive.class));

                SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filterNonStrict.project(LevelThreePrimitive.class));
            }

            @Test
            void longCollection() {
                Type repeated = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT64, ELEMENT);
                GroupType listType = listOfElements(OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelThreePrimitive(String name, List<Long> ids) {
                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.project(LevelThreePrimitive.class));
            }

            @Test
            void floatCollection() {
                Type repeated = new PrimitiveType(OPTIONAL, PrimitiveTypeName.FLOAT, ELEMENT);
                GroupType listType = listOfElements(OPTIONAL, "values", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelThreePrimitive(String name, List<Float> values) {
                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.project(LevelThreePrimitive.class));
            }

            @Test
            void floatFromDoubleCollection() {
                Type repeated = new PrimitiveType(OPTIONAL, PrimitiveTypeName.DOUBLE, ELEMENT);
                GroupType listType = listOfElements(OPTIONAL, "values", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelThreePrimitive(String name, List<Float> values) {
                }
                SchemaFilter filter = new SchemaFilter(strictNumericConfig, groupType);
                assertThrows(RecordTypeConversionException.class, () -> filter.project(LevelThreePrimitive.class));

                SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filterNonStrict.project(LevelThreePrimitive.class));
            }

            @Test
            void doubleCollection() {
                Type repeated = new PrimitiveType(OPTIONAL, PrimitiveTypeName.DOUBLE, ELEMENT);
                GroupType listType = listOfElements(OPTIONAL, "values", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelThreePrimitive(String name, List<Double> values) {
                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.project(LevelThreePrimitive.class));
            }

            @Test
            void stringCollection() {
                Type repeated = Types.primitive(BINARY, OPTIONAL).as(stringType()).named(ELEMENT);
                GroupType listType = listOfElements(OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelThreePrimitive(String name, List<String> ids) {
                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.project(LevelThreePrimitive.class));
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
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.project(LevelThreePrimitive.class));
            }

            @Test
            void enumToStringCollection() {
                Type repeated = Types.primitive(BINARY, OPTIONAL).as(enumType()).named(ELEMENT);
                GroupType listType = listOfElements(OPTIONAL, "categories", repeated);
                GroupType groupType = new MessageType("foo", fieldName, listType);

                record LevelThreePrimitive(String name, List<String> categories) {

                }
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.project(LevelThreePrimitive.class));
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

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                assertEquals(groupType, filter.project(LevelThreeComposite.class));
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

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

                GroupType repeatedExpected = new GroupType(OPTIONAL, ELEMENT, fieldId, fieldAge);
                GroupType listTypeExpected = listOfElements(OPTIONAL, "child", repeatedExpected);
                GroupType groupTypeExpected = new MessageType("foo", fieldName, listTypeExpected);
                assertEquals(groupTypeExpected, filter.project(LevelThreeComposite.class));
            }

            @Test
            void nestedCollectionsWithPrimitivesAreSupported() {
                Type repeatedChild = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, ELEMENT);
                GroupType listType = listOfElements(REPEATED, ELEMENT, repeatedChild);
                GroupType repeated = listOfElements(OPTIONAL, "values", listType);
                GroupType groupType = new MessageType("foo", fieldName, repeated);
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

                record NestedCollections(String name, List<List<Integer>> values) {
                }
                assertEquals(groupType, filter.project(NestedCollections.class));

                record Nested(List<Integer> nested) {
                }
                record ValidLevelThreeComposite(String name, List<Nested> list) {
                }
                assertThrows(CarpetMissingColumnException.class, () -> filter.project(ValidLevelThreeComposite.class));
            }

            @Test
            void nestedCollectionsWithGroupAreSupported() {
                GroupType repeatedChild = new GroupType(OPTIONAL, ELEMENT, fieldId, fieldAge);
                GroupType listType = listOfElements(REPEATED, ELEMENT, repeatedChild);
                GroupType repeated = listOfElements(OPTIONAL, "values", listType);
                GroupType groupType = new MessageType("foo", fieldName, repeated);
                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

                record Child(String id, int age) {
                }
                record NestedCollections(String name, List<List<Child>> values) {
                }
                assertEquals(groupType, filter.project(NestedCollections.class));

                record Nested(List<Child> nested) {
                }
                record ValidLevelThreeComposite(String name, List<Nested> values) {
                }
                assertThrows(CarpetMissingColumnException.class, () -> filter.project(ValidLevelThreeComposite.class));
            }

            @Test
            void collectionCanBeFiltered() {
                Type repeated = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, ELEMENT);
                GroupType listType = listOfElements(OPTIONAL, "ids", repeated);
                GroupType groupType = new MessageType("foo", fieldName, fieldId, fieldActive, listType);

                record LevelThreePrimitive(String id, String name, boolean active) {
                }

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                GroupType expected = new MessageType("foo", fieldName, fieldId, fieldActive);
                assertEquals(expected, filter.project(LevelThreePrimitive.class));
            }

            @Test
            void nestedMapsAreSupported() {
                Type key = Types.primitive(BINARY, REQUIRED).as(stringType()).named(MAP_KEY);
                Type value = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, MAP_VALUE);
                Type mapType = Types.map(REPEATED).key(key).value(value).named(ELEMENT);
                GroupType repeated = listOfElements(OPTIONAL, "values", mapType);
                GroupType groupType = new MessageType("foo", fieldName, repeated);

                SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

                record NestedMap(String name, List<Map<String, Integer>> values) {
                }
                assertEquals(groupType, filter.project(NestedMap.class));
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

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                    assertEquals(groupType, filter.project(MapValuePrimitive.class));
                }

                @Test
                void shortValue() {
                    Type value = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, MAP_VALUE);
                    Type mapType = Types.map(OPTIONAL).key(mapKey).value(value).named("ids");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapValuePrimitive(String name, Map<String, Short> ids) {
                    }

                    SchemaFilter filter = new SchemaFilter(strictNumericConfig, groupType);
                    assertThrows(RecordTypeConversionException.class, () -> filter.project(MapValuePrimitive.class));

                    SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, groupType);
                    assertEquals(groupType, filterNonStrict.project(MapValuePrimitive.class));
                }

                @Test
                void byteValue() {
                    Type value = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, MAP_VALUE);
                    Type mapType = Types.map(OPTIONAL).key(mapKey).value(value).named("ids");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapValuePrimitive(String name, Map<String, Byte> ids) {
                    }

                    SchemaFilter filter = new SchemaFilter(strictNumericConfig, groupType);
                    assertThrows(RecordTypeConversionException.class, () -> filter.project(MapValuePrimitive.class));

                    SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, groupType);
                    assertEquals(groupType, filterNonStrict.project(MapValuePrimitive.class));
                }

                @Test
                void longValue() {
                    Type value = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT64, MAP_VALUE);
                    Type mapType = Types.map(OPTIONAL).key(mapKey).value(value).named("ids");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapValuePrimitive(String name, Map<String, Long> ids) {
                    }

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                    assertEquals(groupType, filter.project(MapValuePrimitive.class));
                }

                @Test
                void floatValue() {
                    Type value = new PrimitiveType(OPTIONAL, PrimitiveTypeName.FLOAT, MAP_VALUE);
                    Type mapType = Types.map(OPTIONAL).key(mapKey).value(value).named("values");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapValuePrimitive(String name, Map<String, Float> values) {
                    }

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                    assertEquals(groupType, filter.project(MapValuePrimitive.class));
                }

                @Test
                void floatFromDoubleValue() {
                    Type value = new PrimitiveType(OPTIONAL, PrimitiveTypeName.DOUBLE, MAP_VALUE);
                    Type mapType = Types.map(OPTIONAL).key(mapKey).value(value).named("values");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapValuePrimitive(String name, Map<String, Float> values) {
                    }

                    SchemaFilter filter = new SchemaFilter(strictNumericConfig, groupType);
                    assertThrows(RecordTypeConversionException.class, () -> filter.project(MapValuePrimitive.class));

                    SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, groupType);
                    assertEquals(groupType, filterNonStrict.project(MapValuePrimitive.class));
                }

                @Test
                void doubleValue() {
                    Type value = new PrimitiveType(OPTIONAL, PrimitiveTypeName.DOUBLE, MAP_VALUE);
                    Type mapType = Types.map(OPTIONAL).key(mapKey).value(value).named("values");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapValuePrimitive(String name, Map<String, Double> values) {
                    }

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                    assertEquals(groupType, filter.project(MapValuePrimitive.class));
                }

                @Test
                void stringValue() {
                    Type value = Types.primitive(BINARY, OPTIONAL).as(stringType()).named(MAP_VALUE);
                    Type mapType = Types.map(OPTIONAL).key(mapKey).value(value).named("ids");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapValuePrimitive(String name, Map<String, String> ids) {
                    }

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                    assertEquals(groupType, filter.project(MapValuePrimitive.class));
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

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                    assertEquals(groupType, filter.project(MapValuePrimitive.class));
                }

                @Test
                void enumToStringValue() {
                    Type value = Types.primitive(BINARY, OPTIONAL).as(enumType()).named(MAP_VALUE);
                    Type mapType = Types.map(OPTIONAL).key(mapKey).value(value).named("categories");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapValuePrimitive(String name, Map<String, String> categories) {
                    }

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                    assertEquals(groupType, filter.project(MapValuePrimitive.class));
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

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                    assertEquals(groupType, filter.project(MapValueComposite.class));
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

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

                    Type valueExpected = new GroupType(OPTIONAL, MAP_VALUE, fieldId, fieldAge);
                    Type mapTypeExpected = Types.map(OPTIONAL).key(mapKey).value(valueExpected)
                            .named("child");
                    GroupType groupTypeExpected = new MessageType("foo", fieldName, mapTypeExpected);
                    assertEquals(groupTypeExpected, filter.project(MapValueComposite.class));
                }

                @Test
                void nestedMapValueWithPrimitivesAreSupported() {
                    Type value = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, MAP_VALUE);
                    Type innerMapType = Types.map(OPTIONAL).key(mapKey).value(value).named(MAP_VALUE);
                    Type firstMapType = Types.map(OPTIONAL).key(mapKey).value(innerMapType).named("values");
                    GroupType groupType = new MessageType("foo", fieldName, firstMapType);

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

                    record NestedMaps(String name, Map<String, Map<String, Integer>> values) {
                    }
                    assertEquals(groupType, filter.project(NestedMaps.class));

                    record Nested(Map<String, Integer> nested) {
                    }
                    record InvalidNestedComposite(String name, Map<String, Nested> list) {
                    }
                    assertThrows(CarpetMissingColumnException.class,
                            () -> filter.project(InvalidNestedComposite.class));
                }

                @Test
                void nestedMapValueWithGroupAreSupported() {
                    GroupType groupChild = new GroupType(OPTIONAL, ELEMENT, fieldId, fieldAge);
                    GroupType innerMapType = Types.map(OPTIONAL).key(mapKey).value(groupChild).named(MAP_VALUE);
                    GroupType firstMapType = Types.map(OPTIONAL).key(mapKey).value(innerMapType).named("values");
                    GroupType groupType = new MessageType("foo", fieldName, firstMapType);
                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

                    record Child(String id, int age) {
                    }
                    record NestedMaps(String name, Map<String, Map<String, Child>> values) {
                    }
                    assertEquals(groupType, filter.project(NestedMaps.class));

                    record Nested(Map<String, Child> nested) {
                    }
                    record InvalidNestedComposite(String name, Map<String, Nested> list) {
                    }
                    assertThrows(CarpetMissingColumnException.class,
                            () -> filter.project(InvalidNestedComposite.class));
                }

                @Test
                void nestedMapValueWithGroupMatching() {
                    GroupType groupChild = new GroupType(OPTIONAL, ELEMENT, fieldId, fieldAge, fieldActive);
                    GroupType innerMapType = Types.map(OPTIONAL).key(mapKey).value(groupChild).named(MAP_VALUE);
                    GroupType firstMapType = Types.map(OPTIONAL).key(mapKey).value(innerMapType).named("values");
                    GroupType groupType = new MessageType("foo", fieldName, firstMapType);
                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

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
                    assertEquals(expectedGroupType, filter.project(NestedMaps.class));
                }

                @Test
                void nestedCollectionValueWithPrimitivesAreSupported() {
                    Type value = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, ELEMENT);
                    GroupType innerListType = listOfElements(REPEATED, MAP_VALUE, value);
                    Type firstMapType = Types.map(OPTIONAL).key(mapKey).value(innerListType).named("values");
                    GroupType groupType = new MessageType("foo", fieldName, firstMapType);

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

                    record NestedList(String name, Map<String, List<Integer>> values) {
                    }
                    assertEquals(groupType, filter.project(NestedList.class));

                    record Nested(List<Integer> nested) {
                    }
                    record InvalidNestedComposite(String name, Map<String, Nested> list) {
                    }
                    assertThrows(CarpetMissingColumnException.class,
                            () -> filter.project(InvalidNestedComposite.class));
                }

                @Test
                void nestedCollectionValueWithGroupAreSupported() {
                    GroupType value = new GroupType(OPTIONAL, ELEMENT, fieldId, fieldAge);
                    GroupType innerListType = listOfElements(REPEATED, MAP_VALUE, value);
                    Type firstMapType = Types.map(OPTIONAL).key(mapKey).value(innerListType).named("values");
                    GroupType groupType = new MessageType("foo", fieldName, firstMapType);

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

                    record Child(String id, int age) {
                    }
                    record NestedList(String name, Map<String, List<Child>> values) {
                    }
                    assertEquals(groupType, filter.project(NestedList.class));

                    record Nested(Map<String, List<Child>> nested) {
                    }
                    record InvalidNestedComposite(String name, Map<String, Nested> list) {
                    }
                    assertThrows(CarpetMissingColumnException.class,
                            () -> filter.project(InvalidNestedComposite.class));
                }

                @Test
                void nestedCollectionValueWithGroupMatching() {
                    GroupType value = new GroupType(OPTIONAL, ELEMENT, fieldId, fieldAge, fieldActive);
                    GroupType innerListType = listOfElements(REPEATED, MAP_VALUE, value);
                    Type firstMapType = Types.map(OPTIONAL).key(mapKey).value(innerListType).named("values");
                    GroupType groupType = new MessageType("foo", fieldName, firstMapType);

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

                    record Child(String id, int age) {
                    }
                    record NestedList(String name, Map<String, List<Child>> values) {
                    }

                    GroupType expectedValue = new GroupType(OPTIONAL, ELEMENT, fieldId, fieldAge);
                    GroupType expectedInnerListType = listOfElements(REPEATED, MAP_VALUE, expectedValue);
                    Type expectedFirstMapType = Types.map(OPTIONAL).key(mapKey).value(expectedInnerListType)
                            .named("values");
                    GroupType expectedGroupType = new MessageType("foo", fieldName, expectedFirstMapType);
                    assertEquals(expectedGroupType, filter.project(NestedList.class));
                }

                @Test
                void mapCanBeFiltered() {
                    Type value = new PrimitiveType(OPTIONAL, PrimitiveTypeName.INT32, MAP_VALUE);
                    Type mapType = Types.map(OPTIONAL).key(mapKey).value(value).named("ids");
                    GroupType groupType = new MessageType("foo", fieldName, fieldActive, mapType);

                    record MapFiltered(String name, boolean active) {
                    }

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

                    GroupType expected = new MessageType("foo", fieldName, fieldActive);
                    assertEquals(expected, filter.project(MapFiltered.class));
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

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                    assertEquals(groupType, filter.project(MapKeyPrimitive.class));
                }

                @Test
                void shortKey() {
                    Type key = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, MAP_KEY);
                    Type mapType = Types.map(OPTIONAL).key(key).value(value).named("ids");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapKeyPrimitive(String name, Map<Short, String> ids) {
                    }

                    SchemaFilter filter = new SchemaFilter(strictNumericConfig, groupType);
                    assertThrows(RecordTypeConversionException.class, () -> filter.project(MapKeyPrimitive.class));

                    SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, groupType);
                    assertEquals(groupType, filterNonStrict.project(MapKeyPrimitive.class));
                }

                @Test
                void byteKey() {
                    Type key = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, MAP_KEY);
                    Type mapType = Types.map(OPTIONAL).key(key).value(value).named("ids");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapKeyPrimitive(String name, Map<Byte, String> ids) {
                    }

                    SchemaFilter filter = new SchemaFilter(strictNumericConfig, groupType);
                    assertThrows(RecordTypeConversionException.class, () -> filter.project(MapKeyPrimitive.class));

                    SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, groupType);
                    assertEquals(groupType, filterNonStrict.project(MapKeyPrimitive.class));
                }

                @Test
                void longKey() {
                    Type key = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT64, MAP_KEY);
                    Type mapType = Types.map(OPTIONAL).key(key).value(value).named("ids");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapKeyPrimitive(String name, Map<Long, String> ids) {
                    }

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                    assertEquals(groupType, filter.project(MapKeyPrimitive.class));
                }

                @Test
                void floatKey() {
                    Type key = new PrimitiveType(REQUIRED, PrimitiveTypeName.FLOAT, MAP_KEY);
                    Type mapType = Types.map(OPTIONAL).key(key).value(value).named("ids");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapKeyPrimitive(String name, Map<Float, String> ids) {
                    }

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                    assertEquals(groupType, filter.project(MapKeyPrimitive.class));
                }

                @Test
                void floatFromDoubleKey() {
                    Type key = new PrimitiveType(REQUIRED, PrimitiveTypeName.DOUBLE, MAP_KEY);
                    Type mapType = Types.map(OPTIONAL).key(key).value(value).named("ids");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapKeyPrimitive(String name, Map<Float, String> ids) {
                    }

                    SchemaFilter filter = new SchemaFilter(strictNumericConfig, groupType);
                    assertThrows(RecordTypeConversionException.class, () -> filter.project(MapKeyPrimitive.class));

                    SchemaFilter filterNonStrict = new SchemaFilter(defaultReadConfig, groupType);
                    assertEquals(groupType, filterNonStrict.project(MapKeyPrimitive.class));
                }

                @Test
                void doubleKey() {
                    Type key = new PrimitiveType(REQUIRED, PrimitiveTypeName.DOUBLE, MAP_KEY);
                    Type mapType = Types.map(OPTIONAL).key(key).value(value).named("ids");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapKeyPrimitive(String name, Map<Double, String> ids) {
                    }

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                    assertEquals(groupType, filter.project(MapKeyPrimitive.class));
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

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                    assertEquals(groupType, filter.project(MapKeyPrimitive.class));
                }

                @Test
                void enumToStringKey() {
                    Type key = Types.primitive(BINARY, REQUIRED).as(enumType()).named(MAP_KEY);
                    Type mapType = Types.map(OPTIONAL).key(key).value(value).named("ids");
                    GroupType groupType = new MessageType("foo", fieldName, mapType);

                    record MapKeyPrimitive(String name, Map<String, String> ids) {
                    }

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                    assertEquals(groupType, filter.project(MapKeyPrimitive.class));
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

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
                    assertEquals(groupType, filter.project(MapValueComposite.class));
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

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

                    Type expectedKey = new GroupType(REQUIRED, MAP_KEY, fieldId, fieldAge);
                    Type expectedMapType = Types.map(OPTIONAL).key(expectedKey).value(value).named("child");
                    GroupType groupTypeExpected = new MessageType("foo", fieldName, expectedMapType);
                    assertEquals(groupTypeExpected, filter.project(MapValueComposite.class));
                }

                @Test
                void nestedMapsKeyAreNotSupported() {
                    Type nestedValueId = Types.primitive(BINARY, REQUIRED).as(stringType()).named(MAP_KEY);
                    Type innerMapKeyType = Types.map(REQUIRED).key(nestedValueId).value(value).named(MAP_KEY);
                    Type count = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, MAP_VALUE);
                    Type firstMapType = Types.map(OPTIONAL).key(innerMapKeyType).value(count).named("values");
                    GroupType groupType = new MessageType("foo", fieldName, firstMapType);

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

                    record NestedMaps(String name, Map<Map<String, Integer>, Integer> values) {
                    }
                    assertThrows(RecordTypeConversionException.class, () -> filter.project(NestedMaps.class));
                }

                @Test
                void nestedCollectionsKeyAreNotSupported() {
                    Type nestedValueId = Types.primitive(BINARY, REQUIRED).as(stringType()).named(ELEMENT);
                    Type innerListKeyType = listOfElements(REQUIRED, MAP_KEY, nestedValueId);
                    Type count = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, MAP_VALUE);
                    Type firstMapType = Types.map(OPTIONAL).key(innerListKeyType).value(count).named("values");
                    GroupType groupType = new MessageType("foo", fieldName, firstMapType);

                    SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);

                    record NestedMaps(String name, Map<List<String>, Integer> values) {
                    }
                    assertThrows(RecordTypeConversionException.class, () -> filter.project(NestedMaps.class));
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

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
            assertEquals(new MessageType("foo", fieldName, fieldId), filter.project(Main.class));
        }

        @Test
        void ifFieldIsMissingValidationFails() {
            Type fieldName = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type fieldId = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            Type fieldAge = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType groupType = new MessageType("foo", fieldName, fieldId, fieldAge);

            record Main(String id, String name, int age, String missingField) {
            }

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
            assertThrows(CarpetMissingColumnException.class, () -> filter.project(Main.class));
        }

        @Test
        void ifFieldIsMissingValidationDoesntFailIfIgnoreUnknown() {
            Type fieldName = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type fieldId = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            Type fieldAge = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType groupType = new MessageType("foo", fieldName, fieldId, fieldAge);

            record Main(String id, String name, int age, String missingField) {
            }

            SchemaFilter filter = new SchemaFilter(dontFailOnMissingFields, groupType);
            assertEquals(groupType, filter.project(Main.class));
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

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
            assertThrows(CarpetMissingColumnException.class, () -> filter.project(CompositeMain.class));
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

            SchemaFilter filter = new SchemaFilter(dontFailOnMissingFields, groupType);
            assertEquals(groupType, filter.project(CompositeMain.class));
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

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
            assertThrows(CarpetMissingColumnException.class, () -> filter.project(CompositeMain.class));
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

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
            assertThrows(RecordTypeConversionException.class, () -> filter.project(Main.class));
        }

        @Test
        void stringIsIncompatibleWithNumeric() {
            Type fieldName = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type fieldId = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            Type fieldAge = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType groupType = new MessageType("foo", fieldName, fieldId, fieldAge);

            record Main(String id, int name, int age) {
            }

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
            assertThrows(RecordTypeConversionException.class, () -> filter.project(Main.class));
        }

        @Test
        void primitiveIsIncompatibleWithList() {
            Type fieldName = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type fieldId = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            Type fieldAge = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "age");
            GroupType groupType = new MessageType("foo", fieldName, fieldId, fieldAge);

            record Main(String id, String name, List<Integer> age) {
            }

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
            assertThrows(RecordTypeConversionException.class, () -> filter.project(Main.class));
        }

        @Test
        void listIsIncompatibleWithPrimitive() {
            Type fieldName = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type fieldId = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            Type fieldAge = new PrimitiveType(REQUIRED, PrimitiveTypeName.INT32, "element");
            GroupType listType = listOfElements(OPTIONAL, "ages", fieldAge);
            GroupType groupType = new MessageType("foo", fieldName, fieldId, listType);

            record Main(String id, String name, Integer ages) {
            }

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
            assertThrows(RecordTypeConversionException.class, () -> filter.project(Main.class));
        }

        @Test
        void primitiveIsIncompatibleWithMap() {
            Type fieldName = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("name");
            Type fieldId = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("id");
            Type fieldCode = Types.primitive(BINARY, OPTIONAL).as(stringType()).named("code");
            GroupType groupType = new MessageType("foo", fieldName, fieldId, fieldCode);

            record Main(String id, String name, Map<Integer, String> code) {
            }

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
            assertThrows(RecordTypeConversionException.class, () -> filter.project(Main.class));
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

            SchemaFilter filter = new SchemaFilter(defaultReadConfig, groupType);
            assertThrows(RecordTypeConversionException.class, () -> filter.project(Main.class));
        }

    }

}
