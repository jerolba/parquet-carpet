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

import static com.jerolba.carpet.TimeUnit.MICROS;
import static com.jerolba.carpet.TimeUnit.MILLIS;
import static com.jerolba.carpet.TimeUnit.NANOS;
import static com.jerolba.carpet.impl.write.DecimalConfig.decimalConfig;
import static com.jerolba.carpet.model.FieldTypes.BIG_DECIMAL;
import static com.jerolba.carpet.model.FieldTypes.BINARY;
import static com.jerolba.carpet.model.FieldTypes.BOOLEAN;
import static com.jerolba.carpet.model.FieldTypes.BYTE;
import static com.jerolba.carpet.model.FieldTypes.DOUBLE;
import static com.jerolba.carpet.model.FieldTypes.ENUM;
import static com.jerolba.carpet.model.FieldTypes.FLOAT;
import static com.jerolba.carpet.model.FieldTypes.INSTANT;
import static com.jerolba.carpet.model.FieldTypes.INTEGER;
import static com.jerolba.carpet.model.FieldTypes.LIST;
import static com.jerolba.carpet.model.FieldTypes.LOCAL_DATE;
import static com.jerolba.carpet.model.FieldTypes.LOCAL_DATE_TIME;
import static com.jerolba.carpet.model.FieldTypes.LOCAL_TIME;
import static com.jerolba.carpet.model.FieldTypes.LONG;
import static com.jerolba.carpet.model.FieldTypes.MAP;
import static com.jerolba.carpet.model.FieldTypes.SHORT;
import static com.jerolba.carpet.model.FieldTypes.STRING;
import static com.jerolba.carpet.model.FieldTypes.UUID;
import static com.jerolba.carpet.model.FieldTypes.writeRecordModel;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.jerolba.carpet.AnnotatedLevels;
import com.jerolba.carpet.ColumnNamingStrategy;
import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.TimeUnit;
import com.jerolba.carpet.model.WriteRecordModelType;

class WriteRecord2SchemaTest {

    @Test
    void simpleRecordTest() {
        record SimpleRecord(long id, String name) {
        }

        var rootType = writeRecordModel(SimpleRecord.class)
                .withField("id", LONG.notNull(), SimpleRecord::id)
                .withField("name", STRING, SimpleRecord::name);

        String expected = """
                message SimpleRecord {
                  required int64 id;
                  optional binary name (STRING);
                }
                """;
        assertEquals(expected, schemaWithRootType(rootType).toString());
    }

    @Test
    void privitiveTypesRecordTest() {
        record PrimitiveTypesRecord(long longValue, int intValue, float floatValue, double doubleValue,
                short shortValue, byte byteValue, boolean booleanValue) {
        }

        var rootType = writeRecordModel(PrimitiveTypesRecord.class)
                .withField("longValue", LONG.notNull(), PrimitiveTypesRecord::longValue)
                .withField("intValue", INTEGER.notNull(), PrimitiveTypesRecord::intValue)
                .withField("floatValue", FLOAT.notNull(), PrimitiveTypesRecord::floatValue)
                .withField("doubleValue", DOUBLE.notNull(), PrimitiveTypesRecord::doubleValue)
                .withField("shortValue", SHORT.notNull(), PrimitiveTypesRecord::shortValue)
                .withField("byteValue", BYTE.notNull(), PrimitiveTypesRecord::byteValue)
                .withField("booleanValue", BOOLEAN.notNull(), PrimitiveTypesRecord::booleanValue);

        String expected = """
                message PrimitiveTypesRecord {
                  required int64 longValue;
                  required int32 intValue;
                  required float floatValue;
                  required double doubleValue;
                  required int32 shortValue (INTEGER(16,true));
                  required int32 byteValue (INTEGER(8,true));
                  required boolean booleanValue;
                }
                """;
        assertEquals(expected, schemaWithRootType(rootType).toString());
    }

    @Test
    void privitiveObjectsTypesRecordTest() {
        record PrimitiveObjectsTypesRecord(Long longValue, Integer intValue, Float floatValue, Double doubleValue,
                Short shortValue, Byte byteValue, Boolean booleanValue) {
        }

        var rootType = writeRecordModel(PrimitiveObjectsTypesRecord.class)
                .withField("longValue", LONG, PrimitiveObjectsTypesRecord::longValue)
                .withField("intValue", INTEGER, PrimitiveObjectsTypesRecord::intValue)
                .withField("floatValue", FLOAT, PrimitiveObjectsTypesRecord::floatValue)
                .withField("doubleValue", DOUBLE, PrimitiveObjectsTypesRecord::doubleValue)
                .withField("shortValue", SHORT, PrimitiveObjectsTypesRecord::shortValue)
                .withField("byteValue", BYTE, PrimitiveObjectsTypesRecord::byteValue)
                .withField("booleanValue", BOOLEAN, PrimitiveObjectsTypesRecord::booleanValue);

        String expected = """
                message PrimitiveObjectsTypesRecord {
                  optional int64 longValue;
                  optional int32 intValue;
                  optional float floatValue;
                  optional double doubleValue;
                  optional int32 shortValue (INTEGER(16,true));
                  optional int32 byteValue (INTEGER(8,true));
                  optional boolean booleanValue;
                }
                """;
        assertEquals(expected, schemaWithRootType(rootType).toString());
    }

    @Test
    void notNullFieldRecordTest() {
        record NotNullFieldRecord(Long id, String name) {
        }

        var rootType = writeRecordModel(NotNullFieldRecord.class)
                .withField("id", LONG.notNull(), NotNullFieldRecord::id)
                .withField("name", STRING.notNull(), NotNullFieldRecord::name);

        String expected = """
                message NotNullFieldRecord {
                  required int64 id;
                  required binary name (STRING);
                }
                """;
        assertEquals(expected, schemaWithRootType(rootType).toString());
    }

    @Test
    void binaryAsStringRecordTest() {
        record SimpleRecord(long id, Binary name) {
        }

        var rootType = writeRecordModel(SimpleRecord.class)
                .withField("id", LONG.notNull(), SimpleRecord::id)
                .withField("name", BINARY.asString(), SimpleRecord::name);

        String expected = """
                message SimpleRecord {
                  required int64 id;
                  optional binary name (STRING);
                }
                """;
        assertEquals(expected, schemaWithRootType(rootType).toString());
    }

    @Nested
    class JsonType {

        @Test
        void jsonFieldFromString() {
            record JsonRecord(long id, String value) {
            }

            var rootType = writeRecordModel(JsonRecord.class)
                    .withField("id", LONG.notNull(), JsonRecord::id)
                    .withField("value", STRING.asJson(), JsonRecord::value);

            String expected = """
                    message JsonRecord {
                      required int64 id;
                      optional binary value (JSON);
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void notNullJsonFieldFromString() {
            record JsonRecord(long id, String value) {
            }

            var rootType = writeRecordModel(JsonRecord.class)
                    .withField("id", LONG.notNull(), JsonRecord::id)
                    .withField("value", STRING.asJson().notNull(), JsonRecord::value);

            String expected = """
                    message JsonRecord {
                      required int64 id;
                      required binary value (JSON);
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void jsonFieldFromBinary() {
            record JsonRecord(long id, Binary value) {
            }

            var rootType = writeRecordModel(JsonRecord.class)
                    .withField("id", LONG.notNull(), JsonRecord::id)
                    .withField("value", BINARY.asJson(), JsonRecord::value);

            String expected = """
                    message JsonRecord {
                      required int64 id;
                      optional binary value (JSON);
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void notNullJsonFieldFromBinary() {
            record JsonRecord(long id, Binary value) {
            }

            var rootType = writeRecordModel(JsonRecord.class)
                    .withField("id", LONG.notNull(), JsonRecord::id)
                    .withField("value", BINARY.asJson().notNull(), JsonRecord::value);

            String expected = """
                    message JsonRecord {
                      required int64 id;
                      required binary value (JSON);
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

    }

    @Nested
    class BsonType {

        @Test
        void bsonFieldFromBinary() {
            record BsonRecord(long id, Binary value) {
            }

            var rootType = writeRecordModel(BsonRecord.class)
                    .withField("id", LONG.notNull(), BsonRecord::id)
                    .withField("value", BINARY.asBson(), BsonRecord::value);

            String expected = """
                    message BsonRecord {
                      required int64 id;
                      optional binary value (BSON);
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void notNullBsonFieldFromBinary() {
            record BsonRecord(long id, Binary value) {
            }

            var rootType = writeRecordModel(BsonRecord.class)
                    .withField("id", LONG.notNull(), BsonRecord::id)
                    .withField("value", BINARY.asBson().notNull(), BsonRecord::value);

            String expected = """
                    message BsonRecord {
                      required int64 id;
                      required binary value (BSON);
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

    }

    @Nested
    class DecimalConfiguration {

        @Test
        void recordField() {
            record RecordFieldDecimal(BigDecimal value) {
            }

            var rootType = writeRecordModel(RecordFieldDecimal.class)
                    .withField("value", BIG_DECIMAL, RecordFieldDecimal::value);

            CarpetWriteConfiguration config = config()
                    .decimalConfig(decimalConfig().withPrecionAndScale(20, 4))
                    .build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message RecordFieldDecimal {
                      optional binary value (DECIMAL(20,4));
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void intPrecision() {
            record RecordFieldDecimal(BigDecimal value) {
            }

            var rootType = writeRecordModel(RecordFieldDecimal.class)
                    .withField("value", BIG_DECIMAL, RecordFieldDecimal::value);

            CarpetWriteConfiguration config = config()
                    .decimalConfig(decimalConfig().withPrecionAndScale(9, 4))
                    .build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message RecordFieldDecimal {
                      optional int32 value (DECIMAL(9,4));
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void longPrecision() {
            record RecordFieldDecimal(BigDecimal value) {
            }

            var rootType = writeRecordModel(RecordFieldDecimal.class)
                    .withField("value", BIG_DECIMAL, RecordFieldDecimal::value);

            CarpetWriteConfiguration config = config()
                    .decimalConfig(decimalConfig().withPrecionAndScale(18, 8))
                    .build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message RecordFieldDecimal {
                      optional int64 value (DECIMAL(18,8));
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void collectionValue() {
            record CollectionDecimalValue(List<BigDecimal> value) {
            }

            var rootType = writeRecordModel(CollectionDecimalValue.class)
                    .withField("value", LIST.ofType(BIG_DECIMAL), CollectionDecimalValue::value);

            CarpetWriteConfiguration config = config()
                    .decimalConfig(decimalConfig().withPrecionAndScale(20, 4))
                    .build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message CollectionDecimalValue {
                      optional group value (LIST) {
                        repeated group list {
                          optional binary element (DECIMAL(20,4));
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedCollectionValue() {
            record NestedCollectionDecimalValue(List<List<BigDecimal>> value) {
            }

            var rootType = writeRecordModel(NestedCollectionDecimalValue.class)
                    .withField("value", LIST.ofType(LIST.ofType(BIG_DECIMAL)),
                            NestedCollectionDecimalValue::value);

            CarpetWriteConfiguration config = config()
                    .decimalConfig(decimalConfig().withPrecionAndScale(20, 4))
                    .build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message NestedCollectionDecimalValue {
                      optional group value (LIST) {
                        repeated group list {
                          optional group element (LIST) {
                            repeated group list {
                              optional binary element (DECIMAL(20,4));
                            }
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void mapKeyAndValue() {
            record MapKeyValueDecimals(Map<BigDecimal, BigDecimal> value) {
            }

            var rootType = writeRecordModel(MapKeyValueDecimals.class)
                    .withField("value", MAP.ofTypes(BIG_DECIMAL, BIG_DECIMAL),
                            MapKeyValueDecimals::value);

            CarpetWriteConfiguration config = config()
                    .decimalConfig(decimalConfig().withPrecionAndScale(20, 4))
                    .build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message MapKeyValueDecimals {
                      optional group value (MAP) {
                        repeated group key_value {
                          required binary key (DECIMAL(20,4));
                          optional binary value (DECIMAL(20,4));
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedMapKeyAndValue() {
            record NestedMapKeyValueDecimal(Map<BigDecimal, Map<BigDecimal, BigDecimal>> value) {
            }

            var rootType = writeRecordModel(NestedMapKeyValueDecimal.class)
                    .withField("value", MAP.ofTypes(BIG_DECIMAL, MAP.ofTypes(BIG_DECIMAL, BIG_DECIMAL)),
                            NestedMapKeyValueDecimal::value);

            CarpetWriteConfiguration config = config()
                    .decimalConfig(decimalConfig().withPrecionAndScale(20, 4))
                    .build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message NestedMapKeyValueDecimal {
                      optional group value (MAP) {
                        repeated group key_value {
                          required binary key (DECIMAL(20,4));
                          optional group value (MAP) {
                            repeated group key_value {
                              required binary key (DECIMAL(20,4));
                              optional binary value (DECIMAL(20,4));
                            }
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

    }

    @Test
    void dateTypesRecordTest() {
        record DateTypesRecord(LocalDate localDate, LocalTime localTime, Instant instant, LocalDateTime localDateTime) {
        }

        var rootType = writeRecordModel(DateTypesRecord.class)
                .withField("localDate", LOCAL_DATE, DateTypesRecord::localDate)
                .withField("localTime", LOCAL_TIME, DateTypesRecord::localTime)
                .withField("instant", INSTANT, DateTypesRecord::instant)
                .withField("localDateTime", LOCAL_DATE_TIME, DateTypesRecord::localDateTime);

        String expected = """
                message DateTypesRecord {
                  optional int32 localDate (DATE);
                  optional int32 localTime (TIME(MILLIS,true));
                  optional int64 instant (TIMESTAMP(MILLIS,true));
                  optional int64 localDateTime (TIMESTAMP(MILLIS,false));
                }
                """;
        assertEquals(expected, schemaWithRootType(rootType).toString());
    }

    @Nested
    class TimeDefinition {

        record TimeRecord(LocalTime localTime) {
        }

        @Test
        void millis() {
            var rootType = writeRecordModel(TimeRecord.class)
                    .withField("localTime", LOCAL_TIME, TimeRecord::localTime);

            CarpetWriteConfiguration config = config().timeUnit(MILLIS).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message TimeRecord {
                      optional int32 localTime (TIME(MILLIS,true));
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void micros() {
            var rootType = writeRecordModel(TimeRecord.class)
                    .withField("localTime", LOCAL_TIME, TimeRecord::localTime);

            CarpetWriteConfiguration config = config().timeUnit(MICROS).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message TimeRecord {
                      optional int64 localTime (TIME(MICROS,true));
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nanos() {
            var rootType = writeRecordModel(TimeRecord.class)
                    .withField("localTime", LOCAL_TIME, TimeRecord::localTime);

            CarpetWriteConfiguration config = config().timeUnit(NANOS).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message TimeRecord {
                      optional int64 localTime (TIME(NANOS,true));
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

    }

    @Nested
    class TimeStampDefinition {

        @Nested
        class InstantDefinition {

            record TimeStampRecord(Instant instant) {
            }

            @Test
            void millis() {
                var rootType = writeRecordModel(TimeStampRecord.class)
                        .withField("instant", INSTANT, TimeStampRecord::instant);

                CarpetWriteConfiguration config = config().timeUnit(MILLIS).build();
                MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

                String expected = """
                        message TimeStampRecord {
                          optional int64 instant (TIMESTAMP(MILLIS,true));
                        }
                        """;
                assertEquals(expected, schema.toString());
            }

            @Test
            void micros() {
                var rootType = writeRecordModel(TimeStampRecord.class)
                        .withField("instant", INSTANT, TimeStampRecord::instant);

                CarpetWriteConfiguration config = config().timeUnit(MICROS).build();
                MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

                String expected = """
                        message TimeStampRecord {
                          optional int64 instant (TIMESTAMP(MICROS,true));
                        }
                        """;
                assertEquals(expected, schema.toString());
            }

            @Test
            void nanos() {
                var rootType = writeRecordModel(TimeStampRecord.class)
                        .withField("instant", INSTANT, TimeStampRecord::instant);

                CarpetWriteConfiguration config = config().timeUnit(NANOS).build();
                MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

                String expected = """
                        message TimeStampRecord {
                          optional int64 instant (TIMESTAMP(NANOS,true));
                        }
                        """;
                assertEquals(expected, schema.toString());
            }
        }

        @Nested
        class LocalDateTimeDefinition {

            record TimeStampRecord(LocalDateTime localdDateTime) {
            }

            @Test
            void millis() {
                var rootType = writeRecordModel(TimeStampRecord.class)
                        .withField("localdDateTime", LOCAL_DATE_TIME, TimeStampRecord::localdDateTime);

                CarpetWriteConfiguration config = config().timeUnit(MILLIS).build();
                MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

                String expected = """
                        message TimeStampRecord {
                          optional int64 localdDateTime (TIMESTAMP(MILLIS,false));
                        }
                        """;
                assertEquals(expected, schema.toString());
            }

            @Test
            void micros() {
                var rootType = writeRecordModel(TimeStampRecord.class)
                        .withField("localdDateTime", LOCAL_DATE_TIME, TimeStampRecord::localdDateTime);

                CarpetWriteConfiguration config = config().timeUnit(MICROS).build();
                MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

                String expected = """
                        message TimeStampRecord {
                          optional int64 localdDateTime (TIMESTAMP(MICROS,false));
                        }
                        """;
                assertEquals(expected, schema.toString());
            }

            @Test
            void nanos() {
                var rootType = writeRecordModel(TimeStampRecord.class)
                        .withField("localdDateTime", LOCAL_DATE_TIME, TimeStampRecord::localdDateTime);

                CarpetWriteConfiguration config = config().timeUnit(NANOS).build();
                MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

                String expected = """
                        message TimeStampRecord {
                          optional int64 localdDateTime (TIMESTAMP(NANOS,false));
                        }
                        """;
                assertEquals(expected, schema.toString());
            }
        }
    }

    @Nested
    class SimpleComposition {

        @Test
        void simpleParentChildRecordTest() {
            record ChildRecord(String key, int value) {
            }
            record ParentRecord(long id, String name, ChildRecord foo) {
            }

            var childType = writeRecordModel(ChildRecord.class)
                    .withField("key", STRING, ChildRecord::key)
                    .withField("value", INTEGER.notNull(), ChildRecord::value);

            var rootType = writeRecordModel(ParentRecord.class)
                    .withField("id", LONG.notNull(), ParentRecord::id)
                    .withField("name", STRING, ParentRecord::name)
                    .withField("foo", childType, ParentRecord::foo);

            String expected = """
                    message ParentRecord {
                      required int64 id;
                      optional binary name (STRING);
                      optional group foo {
                        optional binary key (STRING);
                        required int32 value;
                      }
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void notNullChildRecordTest() {
            record ChildRecord(String key, int value) {
            }
            record NotNullChildRecord(long id, String name, ChildRecord foo) {
            }

            var childType = writeRecordModel(ChildRecord.class)
                    .withField("key", STRING, ChildRecord::key)
                    .withField("value", INTEGER.notNull(), ChildRecord::value);

            var rootType = writeRecordModel(NotNullChildRecord.class)
                    .withField("id", LONG.notNull(), NotNullChildRecord::id)
                    .withField("name", STRING, NotNullChildRecord::name)
                    .withField("bar", childType.notNull(), NotNullChildRecord::foo);

            String expected = """
                    message NotNullChildRecord {
                      required int64 id;
                      optional binary name (STRING);
                      required group bar {
                        optional binary key (STRING);
                        required int32 value;
                      }
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        // How can we test recursivity? Can we define two RecordType variables and
        // reference to each other?

    }

    @Nested
    class EnumTypes {

        enum Status {
            ACTIVE, INACTIVE, DELETED
        }

        @Test
        void withEnum() {
            record WithEnum(long id, String name, Status status) {
            }

            var rootType = writeRecordModel(WithEnum.class)
                    .withField("id", LONG.notNull(), WithEnum::id)
                    .withField("name", STRING, WithEnum::name)
                    .withField("status", ENUM.ofType(Status.class), WithEnum::status);

            String expected = """
                    message WithEnum {
                      required int64 id;
                      optional binary name (STRING);
                      optional binary status (ENUM);
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void withNotNullEnum() {
            record WithNotNullEnum(long id, String name, Status status) {
            }

            var rootType = writeRecordModel(WithNotNullEnum.class)
                    .withField("id", LONG.notNull(), WithNotNullEnum::id)
                    .withField("name", STRING, WithNotNullEnum::name)
                    .withField("status", ENUM.ofType(Status.class).notNull(), WithNotNullEnum::status);

            String expected = """
                    message WithNotNullEnum {
                      required int64 id;
                      optional binary name (STRING);
                      required binary status (ENUM);
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void enumAsString() {
            record WithStringEnum(long id, String name, Status status) {
            }

            var rootType = writeRecordModel(WithStringEnum.class)
                    .withField("id", LONG.notNull(), WithStringEnum::id)
                    .withField("name", STRING, WithStringEnum::name)
                    .withField("status", ENUM.ofType(Status.class).asString(), WithStringEnum::status);

            String expected = """
                    message WithStringEnum {
                      required int64 id;
                      optional binary name (STRING);
                      optional binary status (STRING);
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void stringAsEnum() {
            record WithStringEnum(long id, String name, String status) {
            }

            var rootType = writeRecordModel(WithStringEnum.class)
                    .withField("id", LONG.notNull(), WithStringEnum::id)
                    .withField("name", STRING, WithStringEnum::name)
                    .withField("status", STRING.asEnum(), WithStringEnum::status);

            String expected = """
                    message WithStringEnum {
                      required int64 id;
                      optional binary name (STRING);
                      optional binary status (ENUM);
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void binaryAsEnum() {
            record WithBinaryEnum(long id, String name, Binary status) {
            }

            var rootType = writeRecordModel(WithBinaryEnum.class)
                    .withField("id", LONG.notNull(), WithBinaryEnum::id)
                    .withField("name", STRING, WithBinaryEnum::name)
                    .withField("status", BINARY.asEnum(), WithBinaryEnum::status);

            String expected = """
                    message WithBinaryEnum {
                      required int64 id;
                      optional binary name (STRING);
                      optional binary status (ENUM);
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }
    }

    @Nested
    class UuidTypes {

        @Test
        void withUuid() {
            record WithUuid(UUID id, String name) {
            }

            var rootType = writeRecordModel(WithUuid.class)
                    .withField("id", UUID, WithUuid::id)
                    .withField("name", STRING, WithUuid::name);

            String expected = """
                    message WithUuid {
                      optional fixed_len_byte_array(16) id (UUID);
                      optional binary name (STRING);
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void withNotNullEnum() {
            record WithNotNullUuid(UUID id, String name) {
            }

            var rootType = writeRecordModel(WithNotNullUuid.class)
                    .withField("id", UUID.notNull(), WithNotNullUuid::id)
                    .withField("name", STRING, WithNotNullUuid::name);

            String expected = """
                    message WithNotNullUuid {
                      required fixed_len_byte_array(16) id (UUID);
                      optional binary name (STRING);
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

    }

    @Nested
    class NestedCollection1Level {

        @Test
        void nestedSimpleTypeCollection() {
            record SimpleTypeCollection(String id, List<Integer> values) {
            }

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(INTEGER), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.ONE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      repeated int32 values;
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void consecutiveNestedCollections() {
            record ConsecutiveNestedCollection(String id, List<List<Integer>> values) {
            }

            var rootType = writeRecordModel(ConsecutiveNestedCollection.class)
                    .withField("id", STRING, ConsecutiveNestedCollection::id)
                    .withField("values", LIST.ofType(LIST.ofType(INTEGER)), ConsecutiveNestedCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.ONE).build();

            assertThrows(RecordTypeConversionException.class,
                    () -> new WriteRecordModel2Schema(config).createSchema(rootType));
        }

        @Test
        void nonConsecutiveNestedCollections() {
            record ChildCollection(String name, List<String> alias) {
            }
            record NonConsecutiveNestedCollection(String id, List<ChildCollection> values) {
            }

            var child = writeRecordModel(ChildCollection.class)
                    .withField("name", STRING, ChildCollection::name)
                    .withField("alias", LIST.ofType(STRING), ChildCollection::alias);

            var rootType = writeRecordModel(NonConsecutiveNestedCollection.class)
                    .withField("id", STRING, NonConsecutiveNestedCollection::id)
                    .withField("values", LIST.ofType(child), NonConsecutiveNestedCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.ONE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message NonConsecutiveNestedCollection {
                      optional binary id (STRING);
                      repeated group values {
                        optional binary name (STRING);
                        repeated binary alias (STRING);
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedMapInCollection() {
            record MapInCollection(String name, List<Map<String, Integer>> ids) {
            }

            var rootType = writeRecordModel(MapInCollection.class)
                    .withField("name", STRING, MapInCollection::name)
                    .withField("ids", LIST.ofType(MAP.ofTypes(STRING, INTEGER)), MapInCollection::ids);

            var config = config().annotatedLevels(AnnotatedLevels.ONE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message MapInCollection {
                      optional binary name (STRING);
                      repeated group ids (MAP) {
                        repeated group key_value {
                          required binary key (STRING);
                          optional int32 value;
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

    }

    @Nested
    class NestedCollection2Level {

        @Test
        void nestedSimpleTypeCollection() {
            record SimpleTypeCollection(String id, List<Integer> values) {
            }

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(INTEGER), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.TWO).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated int32 element;
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedRecordCollection() {
            record ChildRecord(String id, Boolean loaded) {
            }
            record NestedRecordCollection(String id, List<ChildRecord> values) {
            }

            var childType = writeRecordModel(ChildRecord.class)
                    .withField("id", STRING, ChildRecord::id)
                    .withField("loaded", BOOLEAN, ChildRecord::loaded);

            var rootType = writeRecordModel(NestedRecordCollection.class)
                    .withField("id", STRING, NestedRecordCollection::id)
                    .withField("values", LIST.ofType(childType), NestedRecordCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.TWO).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message NestedRecordCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group element {
                          optional binary id (STRING);
                          optional boolean loaded;
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedOneTupleCollection() {
            record OneTuple(String str) {
            }
            record NestedOneTupleCollection(String id, List<OneTuple> my_list) {
            }

            var childType = writeRecordModel(OneTuple.class)
                    .withField("str", STRING, OneTuple::str);

            var rootType = writeRecordModel(NestedOneTupleCollection.class)
                    .withField("id", STRING, NestedOneTupleCollection::id)
                    .withField("my_list", LIST.ofType(childType), NestedOneTupleCollection::my_list);

            var config = config().annotatedLevels(AnnotatedLevels.TWO).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message NestedOneTupleCollection {
                      optional binary id (STRING);
                      optional group my_list (LIST) {
                        repeated group element {
                          optional binary str (STRING);
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void consecutiveNestedRecordCollection() {
            record ChildRecord(String id, Boolean loaded) {
            }
            record ConsecutiveNestedRecordCollection(String id, List<List<ChildRecord>> values) {
            }

            var childType = writeRecordModel(ChildRecord.class)
                    .withField("id", STRING, ChildRecord::id)
                    .withField("loaded", BOOLEAN, ChildRecord::loaded);

            var rootType = writeRecordModel(ConsecutiveNestedRecordCollection.class)
                    .withField("id", STRING, ConsecutiveNestedRecordCollection::id)
                    .withField("values", LIST.ofType(LIST.ofType(childType)),
                            ConsecutiveNestedRecordCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.TWO).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message ConsecutiveNestedRecordCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group element (LIST) {
                          repeated group element {
                            optional binary id (STRING);
                            optional boolean loaded;
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void consecutiveTripleNestedRecordCollection() {
            record ChildRecord(String id, Boolean loaded) {
            }
            record ConsecutiveTripleNestedRecordCollection(String id, List<List<List<ChildRecord>>> values) {
            }

            var childType = writeRecordModel(ChildRecord.class)
                    .withField("id", STRING, ChildRecord::id)
                    .withField("loaded", BOOLEAN, ChildRecord::loaded);

            var rootType = writeRecordModel(ConsecutiveTripleNestedRecordCollection.class)
                    .withField("id", STRING, ConsecutiveTripleNestedRecordCollection::id)
                    .withField("values", LIST.ofType(LIST.ofType(LIST.ofType(childType))),
                            ConsecutiveTripleNestedRecordCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.TWO).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message ConsecutiveTripleNestedRecordCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group element (LIST) {
                          repeated group element (LIST) {
                            repeated group element {
                              optional binary id (STRING);
                              optional boolean loaded;
                            }
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void consecutiveNestedSimpleTypeCollections() {
            record ConsecutiveNestedCollection(String id, List<List<Integer>> values) {
            }

            var rootType = writeRecordModel(ConsecutiveNestedCollection.class)
                    .withField("id", STRING, ConsecutiveNestedCollection::id)
                    .withField("values", LIST.ofType(LIST.ofType(INTEGER)), ConsecutiveNestedCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.TWO).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message ConsecutiveNestedCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group element (LIST) {
                          repeated int32 element;
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void consecutiveTripleNestedSimpleTypeCollections() {
            record ConsecutiveTripleNestedCollection(String id, List<List<List<Integer>>> values) {
            }

            var rootType = writeRecordModel(ConsecutiveTripleNestedCollection.class)
                    .withField("id", STRING, ConsecutiveTripleNestedCollection::id)
                    .withField("values", LIST.ofType(LIST.ofType(LIST.ofType(INTEGER))),
                            ConsecutiveTripleNestedCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.TWO).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message ConsecutiveTripleNestedCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group element (LIST) {
                          repeated group element (LIST) {
                            repeated int32 element;
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nonConsecutiveNestedCollections() {
            record ChildCollection(String name, List<String> alias) {
            }
            record NonConsecutiveNestedCollection(String id, List<ChildCollection> values) {
            }

            var childType = writeRecordModel(ChildCollection.class)
                    .withField("name", STRING, ChildCollection::name)
                    .withField("alias", LIST.ofType(STRING), ChildCollection::alias);

            var rootType = writeRecordModel(NonConsecutiveNestedCollection.class)
                    .withField("id", STRING, NonConsecutiveNestedCollection::id)
                    .withField("values", LIST.ofType(childType), NonConsecutiveNestedCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.TWO).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message NonConsecutiveNestedCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group element {
                          optional binary name (STRING);
                          optional group alias (LIST) {
                            repeated binary element (STRING);
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedMapInCollection() {
            record MapInCollection(String name, List<Map<String, Integer>> ids) {
            }

            var rootType = writeRecordModel(MapInCollection.class)
                    .withField("name", STRING, MapInCollection::name)
                    .withField("ids", LIST.ofType(MAP.ofTypes(STRING, INTEGER)), MapInCollection::ids);

            var config = config().annotatedLevels(AnnotatedLevels.TWO).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message MapInCollection {
                      optional binary name (STRING);
                      optional group ids (LIST) {
                        repeated group element (MAP) {
                          repeated group key_value {
                            required binary key (STRING);
                            optional int32 value;
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

    }

    @Nested
    class NestedCollection3Level {

        @Test
        void nestedIntegerCollection() {
            record SimpleTypeCollection(String id, List<Integer> values) {
            }

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(INTEGER), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.THREE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional int32 element;
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedLongCollection() {
            record SimpleTypeCollection(String id, List<Long> values) {
            }

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(LONG), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.THREE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional int64 element;
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedFloatCollection() {
            record SimpleTypeCollection(String id, List<Float> values) {
            }

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(FLOAT), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.THREE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional float element;
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedDoubleCollection() {
            record SimpleTypeCollection(String id, List<Double> values) {
            }

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(DOUBLE), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.THREE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional double element;
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedBooleanCollection() {
            record SimpleTypeCollection(String id, List<Boolean> values) {
            }

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(BOOLEAN), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.THREE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional boolean element;
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedShortCollection() {
            record SimpleTypeCollection(String id, List<Short> values) {
            }

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(SHORT), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.THREE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional int32 element (INTEGER(16,true));
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedByteCollection() {
            record SimpleTypeCollection(String id, List<Byte> values) {
            }

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(BYTE), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.THREE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional int32 element (INTEGER(8,true));
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedRecordCollection() {
            record ChildRecord(String id, Boolean loaded) {
            }
            record NestedRecordCollection(String id, List<ChildRecord> values) {
            }

            var childType = writeRecordModel(ChildRecord.class)
                    .withField("id", STRING, ChildRecord::id)
                    .withField("loaded", BOOLEAN, ChildRecord::loaded);

            var rootType = writeRecordModel(NestedRecordCollection.class)
                    .withField("id", STRING, NestedRecordCollection::id)
                    .withField("values", LIST.ofType(childType), NestedRecordCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.THREE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message NestedRecordCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional group element {
                            optional binary id (STRING);
                            optional boolean loaded;
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void consecutiveNestedRecordCollection() {
            record ChildRecord(String id, Boolean loaded) {
            }
            record ConsecutiveNestedRecordCollection(String id, List<List<ChildRecord>> values) {
            }

            var childType = writeRecordModel(ChildRecord.class)
                    .withField("id", STRING, ChildRecord::id)
                    .withField("loaded", BOOLEAN, ChildRecord::loaded);

            var rootType = writeRecordModel(ConsecutiveNestedRecordCollection.class)
                    .withField("id", STRING, ConsecutiveNestedRecordCollection::id)
                    .withField("values", LIST.ofType(LIST.ofType(childType)),
                            ConsecutiveNestedRecordCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.THREE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message ConsecutiveNestedRecordCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional group element (LIST) {
                            repeated group list {
                              optional group element {
                                optional binary id (STRING);
                                optional boolean loaded;
                              }
                            }
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void consecutiveTripleNestedRecordCollection() {
            record ChildRecord(String id, Boolean loaded) {

            }
            record ConsecutiveTripleNestedRecordCollection(String id, List<List<List<ChildRecord>>> values) {
            }

            var childType = writeRecordModel(ChildRecord.class)
                    .withField("id", STRING, ChildRecord::id)
                    .withField("loaded", BOOLEAN, ChildRecord::loaded);

            var rootType = writeRecordModel(ConsecutiveTripleNestedRecordCollection.class)
                    .withField("id", STRING, ConsecutiveTripleNestedRecordCollection::id)
                    .withField("values", LIST.ofType(LIST.ofType(LIST.ofType(childType))),
                            ConsecutiveTripleNestedRecordCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.THREE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message ConsecutiveTripleNestedRecordCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional group element (LIST) {
                            repeated group list {
                              optional group element (LIST) {
                                repeated group list {
                                  optional group element {
                                    optional binary id (STRING);
                                    optional boolean loaded;
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void consecutiveNestedSimpleTypeCollections() {
            record ConsecutiveNestedCollection(String id, List<List<Integer>> values) {
            }

            var rootType = writeRecordModel(ConsecutiveNestedCollection.class)
                    .withField("id", STRING, ConsecutiveNestedCollection::id)
                    .withField("values", LIST.ofType(LIST.ofType(INTEGER)), ConsecutiveNestedCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.THREE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message ConsecutiveNestedCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional group element (LIST) {
                            repeated group list {
                              optional int32 element;
                            }
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void consecutiveTripleNestedSimpleTypeCollections() {
            record ConsecutiveTripleNestedCollection(String id, List<List<List<Integer>>> values) {
            }

            var rootType = writeRecordModel(ConsecutiveTripleNestedCollection.class)
                    .withField("id", STRING, ConsecutiveTripleNestedCollection::id)
                    .withField("values", LIST.ofType(LIST.ofType(LIST.ofType(INTEGER))),
                            ConsecutiveTripleNestedCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.THREE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message ConsecutiveTripleNestedCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional group element (LIST) {
                            repeated group list {
                              optional group element (LIST) {
                                repeated group list {
                                  optional int32 element;
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nonConsecutiveNestedCollections() {
            record ChildCollection(String name, List<String> alias) {
            }
            record NonConsecutiveNestedCollection(String id, List<ChildCollection> values) {
            }

            var childType = writeRecordModel(ChildCollection.class)
                    .withField("name", STRING, ChildCollection::name)
                    .withField("alias", LIST.ofType(STRING), ChildCollection::alias);

            var rootType = writeRecordModel(NonConsecutiveNestedCollection.class)
                    .withField("id", STRING, NonConsecutiveNestedCollection::id)
                    .withField("values", LIST.ofType(childType), NonConsecutiveNestedCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.THREE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message NonConsecutiveNestedCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional group element {
                            optional binary name (STRING);
                            optional group alias (LIST) {
                              repeated group list {
                                optional binary element (STRING);
                              }
                            }
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void collectionWithNestedMap() {
            record CollectionWithNestedMap(String id, List<Map<String, Integer>> values) {
            }

            var rootType = writeRecordModel(CollectionWithNestedMap.class)
                    .withField("id", STRING, CollectionWithNestedMap::id)
                    .withField("values", LIST.ofType(MAP.ofTypes(STRING, INTEGER)),
                            CollectionWithNestedMap::values);

            var config = config().annotatedLevels(AnnotatedLevels.THREE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message CollectionWithNestedMap {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional group element (MAP) {
                            repeated group key_value {
                              required binary key (STRING);
                              optional int32 value;
                            }
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedMapInCollection() {
            record MapInCollection(String name, List<Map<String, Integer>> ids) {
            }

            var rootType = writeRecordModel(MapInCollection.class)
                    .withField("name", STRING, MapInCollection::name)
                    .withField("ids", LIST.ofType(MAP.ofTypes(STRING, INTEGER)), MapInCollection::ids);

            var config = config().annotatedLevels(AnnotatedLevels.THREE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message MapInCollection {
                      optional binary name (STRING);
                      optional group ids (LIST) {
                        repeated group list {
                          optional group element (MAP) {
                            repeated group key_value {
                              required binary key (STRING);
                              optional int32 value;
                            }
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }
    }

    @Nested
    class NestedMaps {

        @Test
        void nestedSimpleTypeMap() {
            record SimpleTypeMap(String id, Map<String, Integer> values) {
            }

            var rootType = writeRecordModel(SimpleTypeMap.class)
                    .withField("id", STRING, SimpleTypeMap::id)
                    .withField("values", MAP.ofTypes(STRING, INTEGER), SimpleTypeMap::values);

            String expected = """
                    message SimpleTypeMap {
                      optional binary id (STRING);
                      optional group values (MAP) {
                        repeated group key_value {
                          required binary key (STRING);
                          optional int32 value;
                        }
                      }
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void nestedRecordMap() {
            record ChildRecord(String id, Boolean loaded) {
            }
            record NestedRecordMap(String id, Map<String, ChildRecord> values) {
            }

            var childType = writeRecordModel(ChildRecord.class)
                    .withField("id", STRING, ChildRecord::id)
                    .withField("loaded", BOOLEAN, ChildRecord::loaded);

            var rootType = writeRecordModel(NestedRecordMap.class)
                    .withField("id", STRING, NestedRecordMap::id)
                    .withField("values", MAP.ofTypes(STRING, childType), NestedRecordMap::values);

            String expected = """
                    message NestedRecordMap {
                      optional binary id (STRING);
                      optional group values (MAP) {
                        repeated group key_value {
                          required binary key (STRING);
                          optional group value {
                            optional binary id (STRING);
                            optional boolean loaded;
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void mapWithKeyRecord() {
            record KeyRecord(String id, String category) {
            }
            record SimpleTypeMap(String id, Map<KeyRecord, String> values) {
            }

            var childType = writeRecordModel(KeyRecord.class)
                    .withField("id", STRING, KeyRecord::id)
                    .withField("category", STRING, KeyRecord::category);

            var rootType = writeRecordModel(SimpleTypeMap.class)
                    .withField("id", STRING, SimpleTypeMap::id)
                    .withField("values", MAP.ofTypes(childType, STRING), SimpleTypeMap::values);

            String expected = """
                    message SimpleTypeMap {
                      optional binary id (STRING);
                      optional group values (MAP) {
                        repeated group key_value {
                          required group key {
                            optional binary id (STRING);
                            optional binary category (STRING);
                          }
                          optional binary value (STRING);
                        }
                      }
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void consecutiveNestedRecordMap() {
            record ChildRecord(String id, Boolean loaded) {
            }
            record ConsecutiveNestedRecordMap(String id, Map<String, Map<String, ChildRecord>> values) {
            }

            var childType = writeRecordModel(ChildRecord.class)
                    .withField("id", STRING, ChildRecord::id)
                    .withField("loaded", BOOLEAN, ChildRecord::loaded);

            var rootType = writeRecordModel(ConsecutiveNestedRecordMap.class)
                    .withField("id", STRING, ConsecutiveNestedRecordMap::id)
                    .withField("values", MAP.ofTypes(STRING, MAP.ofTypes(STRING, childType)),
                            ConsecutiveNestedRecordMap::values);

            String expected = """
                    message ConsecutiveNestedRecordMap {
                      optional binary id (STRING);
                      optional group values (MAP) {
                        repeated group key_value {
                          required binary key (STRING);
                          optional group value (MAP) {
                            repeated group key_value {
                              required binary key (STRING);
                              optional group value {
                                optional binary id (STRING);
                                optional boolean loaded;
                              }
                            }
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void consecutiveNestedMapCollection() {
            record ChildRecord(String id, Boolean loaded) {
            }
            record ConsecutiveNestedRecordMap(String id, Map<String, List<ChildRecord>> values) {
            }

            var childType = writeRecordModel(ChildRecord.class)
                    .withField("id", STRING, ChildRecord::id)
                    .withField("loaded", BOOLEAN, ChildRecord::loaded);

            var rootType = writeRecordModel(ConsecutiveNestedRecordMap.class)
                    .withField("id", STRING, ConsecutiveNestedRecordMap::id)
                    .withField("values", MAP.ofTypes(STRING, LIST.ofType(childType)),
                            ConsecutiveNestedRecordMap::values);

            String expected = """
                    message ConsecutiveNestedRecordMap {
                      optional binary id (STRING);
                      optional group values (MAP) {
                        repeated group key_value {
                          required binary key (STRING);
                          optional group value (LIST) {
                            repeated group list {
                              optional group element {
                                optional binary id (STRING);
                                optional boolean loaded;
                              }
                            }
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void consecutiveTripleNestedMap() {
            record ChildRecord(String id, Boolean loaded) {
            }
            record ConsecutiveTripleNestedMap(String id, Map<String, Map<String, Map<String, ChildRecord>>> values) {
            }

            var childType = writeRecordModel(ChildRecord.class)
                    .withField("id", STRING, ChildRecord::id)
                    .withField("loaded", BOOLEAN, ChildRecord::loaded);

            var rootType = writeRecordModel(ConsecutiveTripleNestedMap.class)
                    .withField("id", STRING, ConsecutiveTripleNestedMap::id)
                    .withField("values", MAP.ofTypes(STRING, MAP.ofTypes(STRING, MAP.ofTypes(STRING, childType))),
                            ConsecutiveTripleNestedMap::values);

            String expected = """
                    message ConsecutiveTripleNestedMap {
                      optional binary id (STRING);
                      optional group values (MAP) {
                        repeated group key_value {
                          required binary key (STRING);
                          optional group value (MAP) {
                            repeated group key_value {
                              required binary key (STRING);
                              optional group value (MAP) {
                                repeated group key_value {
                                  required binary key (STRING);
                                  optional group value {
                                    optional binary id (STRING);
                                    optional boolean loaded;
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void consecutiveNestedSimpleTypeMap() {
            record ConsecutiveNestedCollection(String id, Map<String, Map<String, Integer>> values) {
            }

            var rootType = writeRecordModel(ConsecutiveNestedCollection.class)
                    .withField("id", STRING, ConsecutiveNestedCollection::id)
                    .withField("values", MAP.ofTypes(STRING, MAP.ofTypes(STRING, INTEGER)),
                            ConsecutiveNestedCollection::values);

            String expected = """
                    message ConsecutiveNestedCollection {
                      optional binary id (STRING);
                      optional group values (MAP) {
                        repeated group key_value {
                          required binary key (STRING);
                          optional group value (MAP) {
                            repeated group key_value {
                              required binary key (STRING);
                              optional int32 value;
                            }
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void consecutiveTripleNestedSimpleTypeMap() {
            record ConsecutiveTripleNestedCollection(String id, Map<String, Map<String, Map<String, Integer>>> values) {
            }

            var rootType = writeRecordModel(ConsecutiveTripleNestedCollection.class)
                    .withField("id", STRING, ConsecutiveTripleNestedCollection::id)
                    .withField("values", MAP.ofTypes(STRING, MAP.ofTypes(STRING, MAP.ofTypes(STRING, INTEGER))),
                            ConsecutiveTripleNestedCollection::values);

            String expected = """
                    message ConsecutiveTripleNestedCollection {
                      optional binary id (STRING);
                      optional group values (MAP) {
                        repeated group key_value {
                          required binary key (STRING);
                          optional group value (MAP) {
                            repeated group key_value {
                              required binary key (STRING);
                              optional group value (MAP) {
                                repeated group key_value {
                                  required binary key (STRING);
                                  optional int32 value;
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void nonConsecutiveNestedMaps() {
            record ChildCollection(String name, Map<Integer, Long> alias) {
            }
            record NonConsecutiveNestedMaps(String id, Map<String, ChildCollection> values) {
            }

            var childType = writeRecordModel(ChildCollection.class)
                    .withField("name", STRING, ChildCollection::name)
                    .withField("alias", MAP.ofTypes(INTEGER, LONG), ChildCollection::alias);

            var rootType = writeRecordModel(NonConsecutiveNestedMaps.class)
                    .withField("id", STRING, NonConsecutiveNestedMaps::id)
                    .withField("values", MAP.ofTypes(STRING, childType), NonConsecutiveNestedMaps::values);

            String expected = """
                    message NonConsecutiveNestedMaps {
                      optional binary id (STRING);
                      optional group values (MAP) {
                        repeated group key_value {
                          required binary key (STRING);
                          optional group value {
                            optional binary name (STRING);
                            optional group alias (MAP) {
                              repeated group key_value {
                                required int32 key;
                                optional int64 value;
                              }
                            }
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

    }

    public static ConfigurationBuilder config() {
        return new ConfigurationBuilder();
    }

    public static CarpetWriteConfiguration defaultConfig() {
        return new ConfigurationBuilder().build();
    }

    public static MessageType schemaWithRootType(WriteRecordModelType<?> rootType) {
        return new WriteRecordModel2Schema(defaultConfig()).createSchema(rootType);
    }

    static class ConfigurationBuilder {

        private AnnotatedLevels annotatedLevels = AnnotatedLevels.THREE;
        private TimeUnit timeUnit = TimeUnit.MILLIS;
        private DecimalConfig decimalConfig;

        public ConfigurationBuilder annotatedLevels(AnnotatedLevels annotatedLevels) {
            this.annotatedLevels = annotatedLevels;
            return this;
        }

        public ConfigurationBuilder timeUnit(TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
            return this;
        }

        public ConfigurationBuilder decimalConfig(DecimalConfig decimalConfig) {
            this.decimalConfig = decimalConfig;
            return this;
        }

        public CarpetWriteConfiguration build() {
            return new CarpetWriteConfiguration(annotatedLevels, ColumnNamingStrategy.FIELD_NAME, timeUnit,
                    decimalConfig);
        }
    }

}
