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
package com.jerolba.carpet.writer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.jerolba.carpet.AnnotatedLevels;
import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.annotation.Alias;
import com.jerolba.carpet.annotation.NotNull;
import com.jerolba.carpet.impl.write.CarpetWriteConfiguration;
import com.jerolba.carpet.impl.write.JavaRecord2Schema;

class JavaRecord2SchemaTest {

    private final CarpetWriteConfiguration default3Levels = new CarpetWriteConfiguration(AnnotatedLevels.THREE);
    private final JavaRecord2Schema defaultConfigSchema = new JavaRecord2Schema(default3Levels);

    @Test
    void simpleRecordTest() {
        record SimpleRecord(long id, String name) {
        }

        MessageType schema = defaultConfigSchema.createSchema(SimpleRecord.class);
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

        MessageType schema = defaultConfigSchema.createSchema(PrimitiveTypesRecord.class);
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

        MessageType schema = defaultConfigSchema.createSchema(PrimitiveObjectsTypesRecord.class);
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
    void fieldAliasRecordTest() {
        record FieldAliasRecord(long id, @Alias("nombre") String name) {
        }

        MessageType schema = defaultConfigSchema.createSchema(FieldAliasRecord.class);
        String expected = """
                message FieldAliasRecord {
                  required int64 id;
                  optional binary nombre (STRING);
                }
                """;
        assertEquals(expected, schema.toString());
    }

    @Test
    void notNullFieldRecordTest() {
        record NotNullFieldRecord(@NotNull Long id, @NotNull String name) {
        }

        MessageType schema = defaultConfigSchema.createSchema(NotNullFieldRecord.class);
        String expected = """
                message NotNullFieldRecord {
                  required int64 id;
                  required binary name (STRING);
                }
                """;
        assertEquals(expected, schema.toString());
    }

    @Nested
    class SimpleComposition {

        private final JavaRecord2Schema schemaFactory = defaultConfigSchema;

        @Test
        void simpleParentChildRecordTest() {
            record ChildRecord(String key, int value) {
            }
            record ParentRecord(long id, String name, ChildRecord foo) {
            }

            MessageType schema = schemaFactory.createSchema(ParentRecord.class);
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

            MessageType schema = schemaFactory.createSchema(NotNullChildRecord.class);
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

            assertThrows(RecordTypeConversionException.class, () -> schemaFactory.createSchema(Recursive.class));
        }

        public record RecursiveChild(String name, RecursiveTransitive child) {
        }

        public record RecursiveTransitive(long id, String name, RecursiveChild child) {
        }

        @Test
        void transitiveRecursivityIsNotAllowed() {
            record Recursive(long id, String name, Recursive child) {
            }

            assertThrows(RecordTypeConversionException.class, () -> schemaFactory.createSchema(Recursive.class));
        }

        @Test
        void genericAreNotAllowed() {
            record WithGeneric<T>(String name, T child) {
            }

            assertThrows(RecordTypeConversionException.class, () -> schemaFactory.createSchema(WithGeneric.class));
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

            MessageType schema = defaultConfigSchema.createSchema(WithEnum.class);
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

            MessageType schema = defaultConfigSchema.createSchema(WithNotNullEnum.class);
            String expected = """
                    message WithNotNullEnum {
                      required int64 id;
                      optional binary name (STRING);
                      required binary state (ENUM);
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

            MessageType schema = defaultConfigSchema.createSchema(WithUuid.class);
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

            MessageType schema = defaultConfigSchema.createSchema(WithNotNullUuid.class);
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

        private final CarpetWriteConfiguration oneLevel = new CarpetWriteConfiguration(AnnotatedLevels.ONE);
        private final JavaRecord2Schema schemaFactory = new JavaRecord2Schema(oneLevel);

        @Test
        void nestedSimpleTypeCollection() {
            record SimpleTypeCollection(String id, List<Integer> values) {
            }

            MessageType schema = schemaFactory.createSchema(SimpleTypeCollection.class);
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

            assertThrows(RecordTypeConversionException.class,
                    () -> schemaFactory.createSchema(ConsecutiveNestedCollection.class));
        }

        @Test
        void nonConsecutiveNestedCollections() {
            record ChildCollection(String name, List<String> alias) {

            }
            record NonConsecutiveNestedCollection(String id, List<ChildCollection> values) {
            }

            MessageType schema = schemaFactory.createSchema(NonConsecutiveNestedCollection.class);
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
                    () -> schemaFactory.createSchema(GenericCollection.class));
        }

        @Test
        void nestedMapInCollection() {
            record MapInCollection(String name, List<Map<String, Integer>> ids) {
            }

            MessageType schema = schemaFactory.createSchema(MapInCollection.class);
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

        private final CarpetWriteConfiguration twoLevel = new CarpetWriteConfiguration(AnnotatedLevels.TWO);
        private final JavaRecord2Schema schemaFactory = new JavaRecord2Schema(twoLevel);

        @Test
        void nestedSimpleTypeCollection() {
            record SimpleTypeCollection(String id, List<Integer> values) {
            }

            MessageType schema = schemaFactory.createSchema(SimpleTypeCollection.class);
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

            MessageType schema = schemaFactory.createSchema(NestedRecordCollection.class);
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
        void consecutiveNestedRecordCollection() {

            record ChildRecord(String id, Boolean loaded) {

            }
            record ConsecutiveNestedRecordCollection(String id, List<List<ChildRecord>> values) {
            }

            MessageType schema = schemaFactory.createSchema(ConsecutiveNestedRecordCollection.class);
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

            MessageType schema = schemaFactory.createSchema(ConsecutiveTripleNestedRecordCollection.class);
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

            MessageType schema = schemaFactory.createSchema(ConsecutiveNestedCollection.class);
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

            MessageType schema = schemaFactory.createSchema(ConsecutiveTripleNestedCollection.class);
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

            MessageType schema = schemaFactory.createSchema(NonConsecutiveNestedCollection.class);
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
                    () -> schemaFactory.createSchema(GenericCollection.class));
        }

        @Test
        void nestedMapInCollection() {
            record MapInCollection(String name, List<Map<String, Integer>> ids) {
            }

            MessageType schema = schemaFactory.createSchema(MapInCollection.class);
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

        private final CarpetWriteConfiguration threeLevel = new CarpetWriteConfiguration(AnnotatedLevels.THREE);
        private final JavaRecord2Schema schemaFactory = new JavaRecord2Schema(threeLevel);

        @Test
        void nestedIntegerCollection() {
            record SimpleTypeCollection(String id, List<Integer> values) {
            }

            MessageType schema = schemaFactory.createSchema(SimpleTypeCollection.class);
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

            MessageType schema = schemaFactory.createSchema(SimpleTypeCollection.class);
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

            MessageType schema = schemaFactory.createSchema(SimpleTypeCollection.class);
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

            MessageType schema = schemaFactory.createSchema(SimpleTypeCollection.class);
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

            MessageType schema = schemaFactory.createSchema(SimpleTypeCollection.class);
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

            MessageType schema = schemaFactory.createSchema(SimpleTypeCollection.class);
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

            MessageType schema = schemaFactory.createSchema(SimpleTypeCollection.class);
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

            MessageType schema = schemaFactory.createSchema(NestedRecordCollection.class);
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

            MessageType schema = schemaFactory.createSchema(ConsecutiveNestedRecordCollection.class);
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

            MessageType schema = schemaFactory.createSchema(ConsecutiveTripleNestedRecordCollection.class);
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

            MessageType schema = schemaFactory.createSchema(ConsecutiveNestedCollection.class);
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

            MessageType schema = schemaFactory.createSchema(ConsecutiveTripleNestedCollection.class);
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

            MessageType schema = schemaFactory.createSchema(NonConsecutiveNestedCollection.class);
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
                    () -> schemaFactory.createSchema(GenericCollection.class));
        }

        @Test
        void collectionWithNestedMap() {
            record CollectionWithNestedMap(String id, List<Map<String, Integer>> values) {
            }

            MessageType schema = schemaFactory.createSchema(CollectionWithNestedMap.class);
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

            MessageType schema = schemaFactory.createSchema(MapInCollection.class);
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

        private final CarpetWriteConfiguration twoLevel = new CarpetWriteConfiguration(AnnotatedLevels.THREE);
        private final JavaRecord2Schema schemaFactory = new JavaRecord2Schema(twoLevel);

        @Test
        void nestedSimpleTypeMap() {
            record SimpleTypeMap(String id, Map<String, Integer> values) {
            }

            MessageType schema = schemaFactory.createSchema(SimpleTypeMap.class);
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
        void nestedRecordMap() {

            record ChildRecord(String id, Boolean loaded) {

            }
            record NestedRecordMap(String id, Map<String, ChildRecord> values) {
            }

            MessageType schema = schemaFactory.createSchema(NestedRecordMap.class);
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

            MessageType schema = schemaFactory.createSchema(SimpleTypeMap.class);
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

            MessageType schema = schemaFactory.createSchema(ConsecutiveNestedRecordMap.class);
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

            MessageType schema = schemaFactory.createSchema(ConsecutiveNestedRecordMap.class);
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

            MessageType schema = schemaFactory.createSchema(ConsecutiveTripleNestedMap.class);
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
        void consecutiveNestedSimpleTypeMap() {
            record ConsecutiveNestedCollection(String id, Map<String, Map<String, Integer>> values) {
            }

            MessageType schema = schemaFactory.createSchema(ConsecutiveNestedCollection.class);
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

            MessageType schema = schemaFactory.createSchema(ConsecutiveTripleNestedCollection.class);
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

            MessageType schema = schemaFactory.createSchema(NonConsecutiveNestedMaps.class);
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
                    () -> schemaFactory.createSchema(GenericMap.class));
        }

        @Test
        void genericValueMapNotSupported() {
            record GenericMap<T>(String id, Map<String, T> values) {
            }
            assertThrows(RecordTypeConversionException.class,
                    () -> schemaFactory.createSchema(GenericMap.class));
        }

    }
}
