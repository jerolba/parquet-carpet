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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Locale.Category;
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
import com.jerolba.carpet.annotation.Alias;
import com.jerolba.carpet.annotation.NotNull;
import com.jerolba.carpet.annotation.ParquetBson;
import com.jerolba.carpet.annotation.ParquetEnum;
import com.jerolba.carpet.annotation.ParquetJson;
import com.jerolba.carpet.annotation.ParquetString;
import com.jerolba.carpet.model.WriteRecordModelType;

class JavaRecordMapper2SchemaTest {

    private final ColumnNamingStrategy defaultNaming = ColumnNamingStrategy.FIELD_NAME;
    private final TimeUnit defaultTimeUnit = MILLIS;
    private final CarpetWriteConfiguration default3Levels = new CarpetWriteConfiguration(AnnotatedLevels.THREE,
            defaultNaming, defaultTimeUnit, null);

    @Test
    void simpleRecordTest() {
        record SimpleRecord(long id, String name) {
        }

        MessageType schema = class2Model2Schema(SimpleRecord.class);
        String expected = """
                message SimpleRecord {
                  required int64 id;
                  optional binary name (STRING);
                }
                """;
        assertEquals(expected, schema.toString());
    }

    @Test
    void privitiveTypesRecordTest() {
        record PrimitiveTypesRecord(long longValue, int intValue, float floatValue, double doubleValue,
                short shortValue, byte byteValue, boolean booleanValue) {
        }

        MessageType schema = class2Model2Schema(PrimitiveTypesRecord.class);
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
        assertEquals(expected, schema.toString());
    }

    @Test
    void privitiveObjectsTypesRecordTest() {
        record PrimitiveObjectsTypesRecord(Long longValue, Integer intValue, Float floatValue, Double doubleValue,
                Short shortValue, Byte byteValue, Boolean booleanValue) {
        }

        MessageType schema = class2Model2Schema(PrimitiveObjectsTypesRecord.class);
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
        assertEquals(expected, schema.toString());
    }

    @Test
    void notNullFieldRecordTest() {
        record NotNullFieldRecord(@NotNull Long id, @NotNull String name) {
        }

        MessageType schema = class2Model2Schema(NotNullFieldRecord.class);
        String expected = """
                message NotNullFieldRecord {
                  required int64 id;
                  required binary name (STRING);
                }
                """;
        assertEquals(expected, schema.toString());
    }

    @Test
    void binaryAsStringRecordTest() {
        record SimpleRecord(long id, @ParquetString Binary name) {
        }

        MessageType schema = class2Model2Schema(SimpleRecord.class);

        String expected = """
                message SimpleRecord {
                  required int64 id;
                  optional binary name (STRING);
                }
                """;
        assertEquals(expected, schema.toString());
    }

    @Test
    void unannotatedBinaryHasNoLogicalType() {
        record SimpleRecord(long id, Binary data) {
        }

        MessageType schema = class2Model2Schema(SimpleRecord.class);

        String expected = """
                message SimpleRecord {
                  required int64 id;
                  optional binary data;
                }
                """;
        assertEquals(expected, schema.toString());
    }

    @Nested
    class JsonType {

        @Test
        void jsonFieldFromString() {
            record JsonRecord(long id, @ParquetJson String value) {
            }
            MessageType schema = class2Model2Schema(JsonRecord.class);
            String expected = """
                    message JsonRecord {
                      required int64 id;
                      optional binary value (JSON);
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void notNullJsonFieldFromString() {
            record JsonRecord(long id, @ParquetJson @NotNull String value) {
            }

            MessageType schema = class2Model2Schema(JsonRecord.class);
            String expected = """
                    message JsonRecord {
                      required int64 id;
                      required binary value (JSON);
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void jsonFieldFromBinary() {
            record JsonRecord(long id, @ParquetJson Binary value) {
            }
            MessageType schema = class2Model2Schema(JsonRecord.class);
            String expected = """
                    message JsonRecord {
                      required int64 id;
                      optional binary value (JSON);
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void notNullJsonFieldFromBinary() {
            record JsonRecord(long id, @ParquetJson @NotNull Binary value) {
            }

            MessageType schema = class2Model2Schema(JsonRecord.class);
            String expected = """
                    message JsonRecord {
                      required int64 id;
                      required binary value (JSON);
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

    }

    @Nested
    class BsonType {

        @Test
        void bsonFieldFromBinary() {
            record BsonRecord(long id, @ParquetBson Binary value) {
            }
            MessageType schema = class2Model2Schema(BsonRecord.class);
            String expected = """
                    message BsonRecord {
                      required int64 id;
                      optional binary value (BSON);
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void notNullBsonFieldFromBinary() {
            record BsonRecord(long id, @ParquetBson @NotNull Binary value) {
            }

            MessageType schema = class2Model2Schema(BsonRecord.class);
            String expected = """
                    message BsonRecord {
                      required int64 id;
                      required binary value (BSON);
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

    }

    @Nested
    class DecimalConfiguration {

        CarpetWriteConfiguration config = new CarpetWriteConfiguration(AnnotatedLevels.THREE,
                defaultNaming, defaultTimeUnit, decimalConfig().withPrecionAndScale(20, 4));

        @Test
        void recordField() {
            record RecordFieldDecimal(BigDecimal value) {
            }

            MessageType schema = class2Model2Schema(RecordFieldDecimal.class, config);
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

            CarpetWriteConfiguration intPrecisionConfig = new CarpetWriteConfiguration(AnnotatedLevels.THREE,
                    defaultNaming, defaultTimeUnit, decimalConfig().withPrecionAndScale(9, 4));

            MessageType schema = class2Model2Schema(RecordFieldDecimal.class, intPrecisionConfig);
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

            CarpetWriteConfiguration longPrecisionConfig = new CarpetWriteConfiguration(AnnotatedLevels.THREE,
                    defaultNaming, defaultTimeUnit, decimalConfig().withPrecionAndScale(18, 8));

            MessageType schema = class2Model2Schema(RecordFieldDecimal.class, longPrecisionConfig);
            String expected = """
                    message RecordFieldDecimal {
                      optional int64 value (DECIMAL(18,8));
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void invalidConfig() {
            assertThrowsExactly(IllegalArgumentException.class, () -> decimalConfig().withPrecionAndScale(0, 8));
            assertThrowsExactly(IllegalArgumentException.class, () -> decimalConfig().withPrecionAndScale(10, -1));
            assertThrowsExactly(IllegalArgumentException.class, () -> decimalConfig().withPrecionAndScale(12, 13));
        }

        @Test
        void collectionValue() {
            record CollectionDecimalValue(List<BigDecimal> value) {
            }

            MessageType schema = class2Model2Schema(CollectionDecimalValue.class, config);
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

            MessageType schema = class2Model2Schema(NestedCollectionDecimalValue.class, config);
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

            MessageType schema = class2Model2Schema(MapKeyValueDecimals.class, config);
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

            MessageType schema = class2Model2Schema(NestedMapKeyValueDecimal.class, config);
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

        MessageType schema = class2Model2Schema(DateTypesRecord.class);
        String expected = """
                message DateTypesRecord {
                  optional int32 localDate (DATE);
                  optional int32 localTime (TIME(MILLIS,true));
                  optional int64 instant (TIMESTAMP(MILLIS,true));
                  optional int64 localDateTime (TIMESTAMP(MILLIS,false));
                }
                """;
        assertEquals(expected, schema.toString());
    }

    @Nested
    class TimeDefinition {

        record TimeRecord(LocalTime localTime) {
        }

        @Test
        void millis() {
            var cfg = new CarpetWriteConfiguration(AnnotatedLevels.THREE, defaultNaming, MILLIS, null);
            MessageType schema = class2Model2Schema(TimeRecord.class, cfg);
            String expected = """
                    message TimeRecord {
                      optional int32 localTime (TIME(MILLIS,true));
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void micros() {
            var cfg = new CarpetWriteConfiguration(AnnotatedLevels.THREE, defaultNaming, MICROS, null);
            MessageType schema = class2Model2Schema(TimeRecord.class, cfg);
            String expected = """
                    message TimeRecord {
                      optional int64 localTime (TIME(MICROS,true));
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nanos() {
            var cfg = new CarpetWriteConfiguration(AnnotatedLevels.THREE, defaultNaming, NANOS, null);
            MessageType schema = class2Model2Schema(TimeRecord.class, cfg);
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
                var cfg = new CarpetWriteConfiguration(AnnotatedLevels.THREE, defaultNaming, MILLIS, null);
                MessageType schema = class2Model2Schema(TimeStampRecord.class, cfg);
                String expected = """
                        message TimeStampRecord {
                          optional int64 instant (TIMESTAMP(MILLIS,true));
                        }
                        """;
                assertEquals(expected, schema.toString());
            }

            @Test
            void micros() {
                var cfg = new CarpetWriteConfiguration(AnnotatedLevels.THREE, defaultNaming, MICROS, null);
                MessageType schema = class2Model2Schema(TimeStampRecord.class, cfg);
                String expected = """
                        message TimeStampRecord {
                          optional int64 instant (TIMESTAMP(MICROS,true));
                        }
                        """;
                assertEquals(expected, schema.toString());
            }

            @Test
            void nanos() {
                var cfg = new CarpetWriteConfiguration(AnnotatedLevels.THREE, defaultNaming, NANOS, null);
                MessageType schema = class2Model2Schema(TimeStampRecord.class, cfg);
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
                var cfg = new CarpetWriteConfiguration(AnnotatedLevels.THREE, defaultNaming, MILLIS, null);
                MessageType schema = class2Model2Schema(TimeStampRecord.class, cfg);
                String expected = """
                        message TimeStampRecord {
                          optional int64 localdDateTime (TIMESTAMP(MILLIS,false));
                        }
                        """;
                assertEquals(expected, schema.toString());
            }

            @Test
            void micros() {
                var cfg = new CarpetWriteConfiguration(AnnotatedLevels.THREE, defaultNaming, MICROS, null);
                MessageType schema = class2Model2Schema(TimeStampRecord.class, cfg);
                String expected = """
                        message TimeStampRecord {
                          optional int64 localdDateTime (TIMESTAMP(MICROS,false));
                        }
                        """;
                assertEquals(expected, schema.toString());
            }

            @Test
            void nanos() {
                var cfg = new CarpetWriteConfiguration(AnnotatedLevels.THREE, defaultNaming, NANOS, null);
                MessageType schema = class2Model2Schema(TimeStampRecord.class, cfg);
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

            MessageType schema = class2Model2Schema(ParentRecord.class);
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
            assertEquals(expected, schema.toString());
        }

        @Test
        void notNullChildRecordTest() {
            record ChildRecord(String key, int value) {
            }
            record NotNullChildRecord(long id, String name, @NotNull @Alias("bar") ChildRecord foo) {
            }

            MessageType schema = class2Model2Schema(NotNullChildRecord.class);
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
            assertEquals(expected, schema.toString());
        }

        @Test
        void recursivityIsNotAllowed() {
            record Recursive(long id, String name, Recursive child) {
            }

            assertThrows(RecordTypeConversionException.class, () -> class2Model2Schema(Recursive.class));
        }

        public record RecursiveChild(String name, RecursiveTransitive child) {
        }

        public record RecursiveTransitive(long id, String name, RecursiveChild child) {
        }

        @Test
        void transitiveRecursivityIsNotAllowed() {
            assertThrows(RecordTypeConversionException.class,
                    () -> class2Model2Schema(RecursiveTransitive.class));
        }

        @Test
        void genericAreNotAllowed() {
            record WithGeneric<T>(String name, T child) {
            }

            assertThrows(RecordTypeConversionException.class, () -> class2Model2Schema(WithGeneric.class));
        }

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

            MessageType schema = class2Model2Schema(WithEnum.class);
            String expected = """
                    message WithEnum {
                      required int64 id;
                      optional binary name (STRING);
                      optional binary status (ENUM);
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void withNotNullEnum() {
            record WithNotNullEnum(long id, String name, @NotNull @Alias("state") Status status) {
            }

            MessageType schema = class2Model2Schema(WithNotNullEnum.class);
            String expected = """
                    message WithNotNullEnum {
                      required int64 id;
                      optional binary name (STRING);
                      required binary state (ENUM);
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void enumAsString() {
            record WithStringEnum(long id, String name, @ParquetString Status status) {
            }

            MessageType schema = class2Model2Schema(WithStringEnum.class);
            String expected = """
                    message WithStringEnum {
                      required int64 id;
                      optional binary name (STRING);
                      optional binary status (STRING);
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void stringAsEnum() {
            record WithStringEnum(long id, String name, @ParquetEnum String status) {
            }

            MessageType schema = class2Model2Schema(WithStringEnum.class);
            String expected = """
                    message WithStringEnum {
                      required int64 id;
                      optional binary name (STRING);
                      optional binary status (ENUM);
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void binaryAsEnum() {
            record WithBinaryEnum(long id, String name, @ParquetEnum Binary status) {
            }

            MessageType schema = class2Model2Schema(WithBinaryEnum.class);
            String expected = """
                    message WithBinaryEnum {
                      required int64 id;
                      optional binary name (STRING);
                      optional binary status (ENUM);
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

    }

    @Nested
    class UuidTypes {

        @Test
        void withUuid() {
            record WithUuid(UUID id, String name) {
            }

            MessageType schema = class2Model2Schema(WithUuid.class);
            String expected = """
                    message WithUuid {
                      optional fixed_len_byte_array(16) id (UUID);
                      optional binary name (STRING);
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void withNotNullEnum() {
            record WithNotNullUuid(@NotNull UUID id, String name) {
            }

            MessageType schema = class2Model2Schema(WithNotNullUuid.class);
            String expected = """
                    message WithNotNullUuid {
                      required fixed_len_byte_array(16) id (UUID);
                      optional binary name (STRING);
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

    }

    @Nested
    class NestedCollection1Level {

        private final CarpetWriteConfiguration oneLevel = new CarpetWriteConfiguration(AnnotatedLevels.ONE,
                defaultNaming, defaultTimeUnit, null);

        @Test
        void nestedSimpleTypeCollection() {
            record SimpleTypeCollection(String id, List<Integer> values) {
            }

            MessageType schema = class2Model2Schema(SimpleTypeCollection.class, oneLevel);
            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      repeated int32 values;
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedStringCollection() {
            record SimpleTypeCollection(String id, List<String> values) {
            }

            MessageType schema = class2Model2Schema(SimpleTypeCollection.class, oneLevel);
            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      repeated binary values (STRING);
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedStringAsEnumCollection() {
            record SimpleTypeCollection(String id, List<@ParquetEnum String> values) {
            }

            MessageType schema = class2Model2Schema(SimpleTypeCollection.class, oneLevel);
            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      repeated binary values (ENUM);
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedEnumCollection() {
            record SimpleTypeCollection(String id, List<Category> values) {
            }

            MessageType schema = class2Model2Schema(SimpleTypeCollection.class, oneLevel);
            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      repeated binary values (ENUM);
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedJsonCollection() {
            record SimpleTypeCollection(String id, List<@ParquetJson String> values) {
            }

            MessageType schema = class2Model2Schema(SimpleTypeCollection.class, oneLevel);
            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      repeated binary values (JSON);
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedBinaryCollection() {
            record SimpleTypeCollection(String id, List<Binary> values) {
            }

            MessageType schema = class2Model2Schema(SimpleTypeCollection.class, oneLevel);
            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      repeated binary values;
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedBsonCollection() {
            record SimpleTypeCollection(String id, List<@ParquetBson Binary> values) {
            }

            MessageType schema = class2Model2Schema(SimpleTypeCollection.class, oneLevel);
            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      repeated binary values (BSON);
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void consecutiveNestedCollections() {
            record ConsecutiveNestedCollection(String id, List<List<Integer>> values) {
            }

            assertThrows(RecordTypeConversionException.class,
                    () -> class2Model2Schema(ConsecutiveNestedCollection.class, oneLevel));
        }

        @Test
        void nonConsecutiveNestedCollections() {
            record ChildCollection(String name, List<String> alias) {

            }
            record NonConsecutiveNestedCollection(String id, List<ChildCollection> values) {
            }

            MessageType schema = class2Model2Schema(NonConsecutiveNestedCollection.class, oneLevel);
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
        void nestedGenericIsNotSupported() {
            record GenericCollection<T>(String id, List<T> values) {
            }
            assertThrows(RecordTypeConversionException.class,
                    () -> class2Model2Schema(GenericCollection.class, oneLevel));
        }

        @Test
        void nestedMapInCollection() {
            record MapInCollection(String name, List<Map<String, Integer>> ids) {
            }

            MessageType schema = class2Model2Schema(MapInCollection.class, oneLevel);
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

        private final CarpetWriteConfiguration twoLevel = new CarpetWriteConfiguration(AnnotatedLevels.TWO,
                defaultNaming, defaultTimeUnit, null);

        @Test
        void nestedSimpleTypeCollection() {
            record SimpleTypeCollection(String id, List<Integer> values) {
            }

            MessageType schema = class2Model2Schema(SimpleTypeCollection.class, twoLevel);
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

            MessageType schema = class2Model2Schema(NestedRecordCollection.class, twoLevel);
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

            MessageType schema = class2Model2Schema(NestedOneTupleCollection.class, twoLevel);
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
        void nestedStringCollection() {
            record SimpleTypeCollection(String id, List<String> values) {
            }

            MessageType schema = class2Model2Schema(SimpleTypeCollection.class, twoLevel);
            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated binary element (STRING);
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedStringAsEnumCollection() {
            record SimpleTypeCollection(String id, List<@ParquetEnum String> values) {
            }

            MessageType schema = class2Model2Schema(SimpleTypeCollection.class, twoLevel);
            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated binary element (ENUM);
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedEnumCollection() {
            record SimpleTypeCollection(String id, List<Category> values) {
            }

            MessageType schema = class2Model2Schema(SimpleTypeCollection.class, twoLevel);
            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated binary element (ENUM);
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedJsonCollection() {
            record SimpleTypeCollection(String id, List<@ParquetJson String> values) {
            }

            MessageType schema = class2Model2Schema(SimpleTypeCollection.class, twoLevel);
            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated binary element (JSON);
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedBinaryCollection() {
            record SimpleTypeCollection(String id, List<Binary> values) {
            }

            MessageType schema = class2Model2Schema(SimpleTypeCollection.class, twoLevel);
            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated binary element;
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedBsonCollection() {
            record SimpleTypeCollection(String id, List<@ParquetBson Binary> values) {
            }

            MessageType schema = class2Model2Schema(SimpleTypeCollection.class, twoLevel);
            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated binary element (BSON);
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

            MessageType schema = class2Model2Schema(ConsecutiveNestedRecordCollection.class, twoLevel);
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

            MessageType schema = class2Model2Schema(ConsecutiveTripleNestedRecordCollection.class, twoLevel);
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

            MessageType schema = class2Model2Schema(ConsecutiveNestedCollection.class, twoLevel);
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

            MessageType schema = class2Model2Schema(ConsecutiveTripleNestedCollection.class, twoLevel);
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

            MessageType schema = class2Model2Schema(NonConsecutiveNestedCollection.class, twoLevel);
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
        void nestedGenericIsNotSupported() {
            record GenericCollection<T>(String id, List<T> values) {
            }
            assertThrows(RecordTypeConversionException.class,
                    () -> class2Model2Schema(GenericCollection.class, twoLevel));
        }

        @Test
        void nestedMapInCollection() {
            record MapInCollection(String name, List<Map<String, Integer>> ids) {
            }

            MessageType schema = class2Model2Schema(MapInCollection.class, twoLevel);
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

            MessageType schema = class2Model2Schema(SimpleTypeCollection.class);
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

            MessageType schema = class2Model2Schema(SimpleTypeCollection.class);
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

            MessageType schema = class2Model2Schema(SimpleTypeCollection.class);
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

            MessageType schema = class2Model2Schema(SimpleTypeCollection.class);
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

            MessageType schema = class2Model2Schema(SimpleTypeCollection.class);
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

            MessageType schema = class2Model2Schema(SimpleTypeCollection.class);
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

            MessageType schema = class2Model2Schema(SimpleTypeCollection.class);
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
        void nestedStringCollection() {
            record SimpleTypeCollection(String id, List<String> values) {
            }

            MessageType schema = class2Model2Schema(SimpleTypeCollection.class);
            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional binary element (STRING);
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedStringAsEnumCollection() {
            record SimpleTypeCollection(String id, List<@ParquetEnum String> values) {
            }

            MessageType schema = class2Model2Schema(SimpleTypeCollection.class);
            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional binary element (ENUM);
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedEnumCollection() {
            record SimpleTypeCollection(String id, List<Category> values) {
            }

            MessageType schema = class2Model2Schema(SimpleTypeCollection.class);
            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional binary element (ENUM);
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedJsonCollection() {
            record SimpleTypeCollection(String id, List<@ParquetJson String> values) {
            }

            MessageType schema = class2Model2Schema(SimpleTypeCollection.class);
            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional binary element (JSON);
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedBinaryCollection() {
            record SimpleTypeCollection(String id, List<Binary> values) {
            }

            MessageType schema = class2Model2Schema(SimpleTypeCollection.class);
            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional binary element;
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedBsonCollection() {
            record SimpleTypeCollection(String id, List<@ParquetBson Binary> values) {
            }

            MessageType schema = class2Model2Schema(SimpleTypeCollection.class);
            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional binary element (BSON);
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

            MessageType schema = class2Model2Schema(NestedRecordCollection.class);
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
        void nestedNotNullStringCollection() {

            record SimpleTypeCollection(@NotNull String id, @NotNull List<@NotNull String> values) {
            }

            MessageType schema = class2Model2Schema(SimpleTypeCollection.class);
            String expected = """
                    message SimpleTypeCollection {
                      required binary id (STRING);
                      required group values (LIST) {
                        repeated group list {
                          required binary element (STRING);
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

            MessageType schema = class2Model2Schema(ConsecutiveNestedRecordCollection.class);
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

            MessageType schema = class2Model2Schema(ConsecutiveTripleNestedRecordCollection.class);
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
        void consecutiveTripleNestedNotNullCollection() {

            record ConsecutiveTripleNestedNotNullCollection(@NotNull String id,
                    @NotNull List<@NotNull List<@NotNull List<@NotNull String>>> values) {
            }

            MessageType schema = class2Model2Schema(ConsecutiveTripleNestedNotNullCollection.class);
            String expected = """
                    message ConsecutiveTripleNestedNotNullCollection {
                      required binary id (STRING);
                      required group values (LIST) {
                        repeated group list {
                          required group element (LIST) {
                            repeated group list {
                              required group element (LIST) {
                                repeated group list {
                                  required binary element (STRING);
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

            MessageType schema = class2Model2Schema(ConsecutiveNestedCollection.class);
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

            MessageType schema = class2Model2Schema(ConsecutiveTripleNestedCollection.class);
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

            MessageType schema = class2Model2Schema(NonConsecutiveNestedCollection.class);
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
        void nestedGenericIsNotSupported() {
            record GenericCollection<T>(String id, List<T> values) {
            }
            assertThrows(RecordTypeConversionException.class,
                    () -> class2Model2Schema(GenericCollection.class));
        }

        @Test
        void collectionWithNestedMap() {
            record CollectionWithNestedMap(String id, List<Map<String, Integer>> values) {
            }

            MessageType schema = class2Model2Schema(CollectionWithNestedMap.class);
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

            MessageType schema = class2Model2Schema(MapInCollection.class);
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

            MessageType schema = class2Model2Schema(SimpleTypeMap.class);
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
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedNotNullableTypeMap() {
            record SimpleTypeMap(@NotNull String id, @NotNull Map<String, @NotNull Integer> values) {
            }

            MessageType schema = class2Model2Schema(SimpleTypeMap.class);
            String expected = """
                    message SimpleTypeMap {
                      required binary id (STRING);
                      required group values (MAP) {
                        repeated group key_value {
                          required binary key (STRING);
                          required int32 value;
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedRecordMap() {

            record ChildRecord(String id, Boolean loaded) {

            }
            record NestedRecordMap(String id, Map<String, ChildRecord> values) {
            }

            MessageType schema = class2Model2Schema(NestedRecordMap.class);
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
            assertEquals(expected, schema.toString());
        }

        @Test
        void mapWithKeyRecord() {
            record KeyRecord(String id, String category) {

            }
            record SimpleTypeMap(String id, Map<KeyRecord, String> values) {
            }

            MessageType schema = class2Model2Schema(SimpleTypeMap.class);
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
            assertEquals(expected, schema.toString());
        }

        @Test
        void consecutiveNestedRecordMap() {

            record ChildRecord(String id, Boolean loaded) {

            }
            record ConsecutiveNestedRecordMap(String id, Map<String, Map<String, ChildRecord>> values) {
            }

            MessageType schema = class2Model2Schema(ConsecutiveNestedRecordMap.class);
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
            assertEquals(expected, schema.toString());
        }

        @Test
        void consecutiveNestedMapCollection() {

            record ChildRecord(String id, Boolean loaded) {

            }
            record ConsecutiveNestedRecordMap(String id, Map<String, List<ChildRecord>> values) {
            }

            MessageType schema = class2Model2Schema(ConsecutiveNestedRecordMap.class);
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
            assertEquals(expected, schema.toString());
        }

        @Test
        void consecutiveTripleNestedMap() {

            record ChildRecord(String id, Boolean loaded) {

            }
            record ConsecutiveTripleNestedMap(String id,
                    Map<String, Map<String, Map<String, ChildRecord>>> values) {
            }

            MessageType schema = class2Model2Schema(ConsecutiveTripleNestedMap.class);
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
            assertEquals(expected, schema.toString());
        }

        @Test
        void consecutiveTripleNestedNotNullMap() {

            record ConsecutiveTripleNestedNotNullMap(@NotNull String id,
                    @NotNull Map<String, @NotNull Map<String, @NotNull Map<String, @NotNull String>>> values) {
            }

            MessageType schema = class2Model2Schema(ConsecutiveTripleNestedNotNullMap.class);
            String expected = """
                    message ConsecutiveTripleNestedNotNullMap {
                      required binary id (STRING);
                      required group values (MAP) {
                        repeated group key_value {
                          required binary key (STRING);
                          required group value (MAP) {
                            repeated group key_value {
                              required binary key (STRING);
                              required group value (MAP) {
                                repeated group key_value {
                                  required binary key (STRING);
                                  required binary value (STRING);
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
        void consecutiveNestedSimpleTypeMap() {
            record ConsecutiveNestedCollection(String id, Map<String, Map<String, Integer>> values) {
            }

            MessageType schema = class2Model2Schema(ConsecutiveNestedCollection.class);
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
            assertEquals(expected, schema.toString());
        }

        @Test
        void consecutiveTripleNestedSimpleTypeMap() {
            record ConsecutiveTripleNestedCollection(String id, Map<String, Map<String, Map<String, Integer>>> values) {
            }

            MessageType schema = class2Model2Schema(ConsecutiveTripleNestedCollection.class);
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
            assertEquals(expected, schema.toString());
        }

        @Test
        void nonConsecutiveNestedMaps() {
            record ChildCollection(String name, Map<Integer, Long> alias) {

            }
            record NonConsecutiveNestedMaps(String id, Map<String, ChildCollection> values) {
            }

            MessageType schema = class2Model2Schema(NonConsecutiveNestedMaps.class);
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
            assertEquals(expected, schema.toString());
        }

        @Test
        void genericKeyMapNotSupported() {
            record GenericMap<T>(String id, Map<T, String> values) {
            }
            assertThrows(RecordTypeConversionException.class,
                    () -> class2Model2Schema(GenericMap.class));
        }

        @Test
        void genericValueMapNotSupported() {
            record GenericMap<T>(String id, Map<String, T> values) {
            }
            assertThrows(RecordTypeConversionException.class,
                    () -> class2Model2Schema(GenericMap.class));
        }

    }

    @Nested
    class ColumnNameConversion {

        @Test
        void fieldAliasRecordTest() {
            record FieldAliasRecord(long id, @Alias("nombre") String name) {
            }

            MessageType schema = class2Model2Schema(FieldAliasRecord.class);
            String expected = """
                    message FieldAliasRecord {
                      required int64 id;
                      optional binary nombre (STRING);
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Nested
        class ToSnakeCase {

            private final CarpetWriteConfiguration snakeCase = new CarpetWriteConfiguration(AnnotatedLevels.THREE,
                    ColumnNamingStrategy.SNAKE_CASE, defaultTimeUnit, null);

            @Test
            void fromCamelCase() {
                record SomeClass(long someId, int operationName, int with3) {
                }

                MessageType schema = class2Model2Schema(SomeClass.class, snakeCase);
                String expected = """
                        message SomeClass {
                          required int64 some_id;
                          required int32 operation_name;
                          required int32 with3;
                        }
                        """;
                assertEquals(expected, schema.toString());
            }

            @Test
            void alreadySnakeCase() {
                record SomeClass(long some_id, int operation_name, int with3) {
                }

                MessageType schema = class2Model2Schema(SomeClass.class, snakeCase);
                String expected = """
                        message SomeClass {
                          required int64 some_id;
                          required int32 operation_name;
                          required int32 with3;
                        }
                        """;
                assertEquals(expected, schema.toString());
            }

        }
    }

    public <T> MessageType class2Model2Schema(Class<T> clzz) {
        return class2Model2Schema(clzz, default3Levels);
    }

    public <T> MessageType class2Model2Schema(Class<T> clzz, CarpetWriteConfiguration config) {
        JavaRecord2WriteModel javaRecord2WriteModel = new JavaRecord2WriteModel(config);
        WriteRecordModelType<T> model = javaRecord2WriteModel.createModel(clzz);
        return new WriteRecordModel2Schema(config).createSchema(model);
    }
}
