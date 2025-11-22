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
import static com.jerolba.carpet.model.FieldTypes.GEOMETRY;
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
import static com.jerolba.carpet.model.FieldTypes.VARIANT;
import static com.jerolba.carpet.model.FieldTypes.writeRecordModel;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Locale.Category;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.parquet.column.schema.EdgeInterpolationAlgorithm;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.variant.Variant;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;

import com.jerolba.carpet.AnnotatedLevels;
import com.jerolba.carpet.ColumnNamingStrategy;
import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.TimeUnit;
import com.jerolba.carpet.annotation.FieldId;
import com.jerolba.carpet.annotation.NotNull;
import com.jerolba.carpet.annotation.ParquetBson;
import com.jerolba.carpet.annotation.ParquetEnum;
import com.jerolba.carpet.annotation.ParquetGeography;
import com.jerolba.carpet.annotation.ParquetGeography.EdgeAlgorithm;
import com.jerolba.carpet.annotation.ParquetGeometry;
import com.jerolba.carpet.annotation.ParquetJson;
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

    @Test
    void unannotatedBinaryHasNoLogicalType() {
        record SimpleRecord(long id, Binary name) {
        }

        var rootType = writeRecordModel(SimpleRecord.class)
                .withField("id", LONG.notNull(), SimpleRecord::id)
                .withField("data", BINARY, SimpleRecord::name);

        String expected = """
                message SimpleRecord {
                  required int64 id;
                  optional binary data;
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
    class GeometryType {

        @Nested
        class FromBinary {

            @Test
            void geometryFieldFromBinaryWithoutCsr() {
                record GeometryRecord(long id, @ParquetGeometry Binary value) {
                }

                var rootType = writeRecordModel(GeometryRecord.class)
                        .withField("id", LONG.notNull(), GeometryRecord::id)
                        .withField("value", BINARY.asParquetGeometry(null), GeometryRecord::value);

                String expected = """
                        message GeometryRecord {
                          required int64 id;
                          optional binary value (GEOMETRY);
                        }
                        """;
                assertEquals(expected, schemaWithRootType(rootType).toString());
            }

            @Test
            void notNullGeometryFieldFromBinary() {
                record GeometryRecord(long id, @ParquetGeometry @NotNull Binary value) {
                }

                var rootType = writeRecordModel(GeometryRecord.class)
                        .withField("id", LONG.notNull(), GeometryRecord::id)
                        .withField("value", BINARY.asParquetGeometry(null).notNull(), GeometryRecord::value);

                String expected = """
                        message GeometryRecord {
                          required int64 id;
                          required binary value (GEOMETRY);
                        }
                        """;
                assertEquals(expected, schemaWithRootType(rootType).toString());
            }

            @Test
            void geometryFieldFromBinaryWithSridCsr() {
                record GeometryRecord(long id, @ParquetGeometry("srid:5070") Binary value) {
                }

                var rootType = writeRecordModel(GeometryRecord.class)
                        .withField("id", LONG.notNull(), GeometryRecord::id)
                        .withField("value", BINARY.asParquetGeometry("srid:5070"), GeometryRecord::value);

                String expected = """
                        message GeometryRecord {
                          required int64 id;
                          optional binary value (GEOMETRY(srid:5070));
                        }
                        """;
                assertEquals(expected, schemaWithRootType(rootType).toString());
            }

            @Test
            void geometryFieldFromBinaryWithProjjsonCsr() {
                record GeometryRecord(long id, @ParquetGeometry("projjson:projjson_epsg_5070") Binary value) {
                }

                var rootType = writeRecordModel(GeometryRecord.class)
                        .withField("id", LONG.notNull(), GeometryRecord::id)
                        .withField("value", BINARY.asParquetGeometry("projjson:projjson_epsg_5070"),
                                GeometryRecord::value);

                String expected = """
                        message GeometryRecord {
                          required int64 id;
                          optional binary value (GEOMETRY(projjson:projjson_epsg_5070));
                        }
                        """;
                assertEquals(expected, schemaWithRootType(rootType).toString());
            }

        }

        @Nested
        class FromGeometry {

            @Test
            void geometryFieldFromGeometryWithoutCsr() {
                record GeometryRecord(long id, @ParquetGeometry Geometry value) {
                }

                var rootType = writeRecordModel(GeometryRecord.class)
                        .withField("id", LONG.notNull(), GeometryRecord::id)
                        .withField("value", GEOMETRY.asParquetGeometry(null), GeometryRecord::value);

                String expected = """
                        message GeometryRecord {
                          required int64 id;
                          optional binary value (GEOMETRY);
                        }
                        """;
                assertEquals(expected, schemaWithRootType(rootType).toString());
            }

            @Test
            void notNullGeometryGeometryFromBinary() {
                record GeometryRecord(long id, @ParquetGeometry @NotNull Geometry value) {
                }

                var rootType = writeRecordModel(GeometryRecord.class)
                        .withField("id", LONG.notNull(), GeometryRecord::id)
                        .withField("value", GEOMETRY.asParquetGeometry(null).notNull(), GeometryRecord::value);

                String expected = """
                        message GeometryRecord {
                          required int64 id;
                          required binary value (GEOMETRY);
                        }
                        """;
                assertEquals(expected, schemaWithRootType(rootType).toString());
            }

            @Test
            void geometryFieldFromGeometryWithSridCsr() {
                record GeometryRecord(long id, @ParquetGeometry("srid:5070") Geometry value) {
                }

                var rootType = writeRecordModel(GeometryRecord.class)
                        .withField("id", LONG.notNull(), GeometryRecord::id)
                        .withField("value", GEOMETRY.asParquetGeometry("srid:5070"), GeometryRecord::value);

                String expected = """
                        message GeometryRecord {
                          required int64 id;
                          optional binary value (GEOMETRY(srid:5070));
                        }
                        """;
                assertEquals(expected, schemaWithRootType(rootType).toString());
            }

            @Test
            void geometryFieldFromGeometryWithProjjsonCsr() {
                record GeometryRecord(long id, @ParquetGeometry("projjson:projjson_epsg_5070") Geometry value) {
                }

                var rootType = writeRecordModel(GeometryRecord.class)
                        .withField("id", LONG.notNull(), GeometryRecord::id)
                        .withField("value", GEOMETRY.asParquetGeometry("projjson:projjson_epsg_5070"),
                                GeometryRecord::value);

                String expected = """
                        message GeometryRecord {
                          required int64 id;
                          optional binary value (GEOMETRY(projjson:projjson_epsg_5070));
                        }
                        """;
                assertEquals(expected, schemaWithRootType(rootType).toString());
            }

        }

    }

    @Nested
    class GeographyType {

        @Nested
        class FromBinary {

            @Test
            void geographyFieldFromBinaryWithoutAnnotatedValues() {
                record GeographyRecord(long id, @ParquetGeography Binary value) {
                }

                var rootType = writeRecordModel(GeographyRecord.class)
                        .withField("id", LONG.notNull(), GeographyRecord::id)
                        .withField("value", BINARY.asParquetGeography(null, null), GeographyRecord::value);

                String expected = """
                        message GeographyRecord {
                          required int64 id;
                          optional binary value (GEOGRAPHY);
                        }
                        """;
                assertEquals(expected, schemaWithRootType(rootType).toString());
            }

            @Test
            void notNullGeographyFieldFromBinary() {
                record GeographyRecord(long id, @ParquetGeography @NotNull Binary value) {
                }

                var rootType = writeRecordModel(GeographyRecord.class)
                        .withField("id", LONG.notNull(), GeographyRecord::id)
                        .withField("value", BINARY.asParquetGeography(null, null).notNull(), GeographyRecord::value);

                String expected = """
                        message GeographyRecord {
                          required int64 id;
                          required binary value (GEOGRAPHY);
                        }
                        """;
                assertEquals(expected, schemaWithRootType(rootType).toString());
            }

            @Test
            void geographyFieldFromBinaryWithSridCsrConfiguresDefaultAlgorithm() {
                record GeographyRecord(long id, @ParquetGeography(crs = "srid:5070") Binary value) {
                }

                var rootType = writeRecordModel(GeographyRecord.class)
                        .withField("id", LONG.notNull(), GeographyRecord::id)
                        .withField("value", BINARY.asParquetGeography("srid:5070", null), GeographyRecord::value);

                String expected = """
                        message GeographyRecord {
                          required int64 id;
                          optional binary value (GEOGRAPHY(srid:5070,SPHERICAL));
                        }
                        """;
                assertEquals(expected, schemaWithRootType(rootType).toString());
            }

            @Test
            void geographyFieldFromBinaryWithAlgorithmConfiguresDefaultCrs() {
                record GeographyRecord(long id, @ParquetGeography(algorithm = EdgeAlgorithm.ANDOYER) Binary value) {
                }

                var rootType = writeRecordModel(GeographyRecord.class)
                        .withField("id", LONG.notNull(), GeographyRecord::id)
                        .withField("value", BINARY.asParquetGeography(null, EdgeInterpolationAlgorithm.ANDOYER),
                                GeographyRecord::value);

                String expected = """
                        message GeographyRecord {
                          required int64 id;
                          optional binary value (GEOGRAPHY(OGC:CRS84,ANDOYER));
                        }
                        """;
                assertEquals(expected, schemaWithRootType(rootType).toString());
            }

        }

        @Nested
        class FromGeometry {

            @Test
            void geographyFieldFromGeometryWithoutAnnotatedValues() {
                record GeographyRecord(long id, @ParquetGeography Geometry value) {
                }

                var rootType = writeRecordModel(GeographyRecord.class)
                        .withField("id", LONG.notNull(), GeographyRecord::id)
                        .withField("value", GEOMETRY.asParquetGeography(null, null), GeographyRecord::value);

                String expected = """
                        message GeographyRecord {
                          required int64 id;
                          optional binary value (GEOGRAPHY);
                        }
                        """;
                assertEquals(expected, schemaWithRootType(rootType).toString());
            }

            @Test
            void notNullGeographyFieldFromGeometry() {
                record GeographyRecord(long id, @ParquetGeography @NotNull Geometry value) {
                }

                var rootType = writeRecordModel(GeographyRecord.class)
                        .withField("id", LONG.notNull(), GeographyRecord::id)
                        .withField("value", GEOMETRY.asParquetGeography(null, null).notNull(), GeographyRecord::value);

                String expected = """
                        message GeographyRecord {
                          required int64 id;
                          required binary value (GEOGRAPHY);
                        }
                        """;
                assertEquals(expected, schemaWithRootType(rootType).toString());
            }

            @Test
            void geographyFieldFromGeometryWithSridCsrConfiguresDefaultAlgorithm() {
                record GeographyRecord(long id, @ParquetGeography(crs = "srid:5070") Geometry value) {
                }

                var rootType = writeRecordModel(GeographyRecord.class)
                        .withField("id", LONG.notNull(), GeographyRecord::id)
                        .withField("value", GEOMETRY.asParquetGeography("srid:5070", null), GeographyRecord::value);

                String expected = """
                        message GeographyRecord {
                          required int64 id;
                          optional binary value (GEOGRAPHY(srid:5070,SPHERICAL));
                        }
                        """;
                assertEquals(expected, schemaWithRootType(rootType).toString());
            }

            @Test
            void geographyFieldFromGeometryWithAlgorithmConfiguresDefaultCrs() {
                record GeographyRecord(long id, @ParquetGeography(algorithm = EdgeAlgorithm.ANDOYER) Geometry value) {
                }

                var rootType = writeRecordModel(GeographyRecord.class)
                        .withField("id", LONG.notNull(), GeographyRecord::id)
                        .withField("value", GEOMETRY.asParquetGeography(null, EdgeInterpolationAlgorithm.ANDOYER),
                                GeographyRecord::value);

                String expected = """
                        message GeographyRecord {
                          required int64 id;
                          optional binary value (GEOGRAPHY(OGC:CRS84,ANDOYER));
                        }
                        """;
                assertEquals(expected, schemaWithRootType(rootType).toString());
            }

        }

    }

    @Nested
    class VariantSupport {

        @Test
        void recordWithVariant() {
            record RecordWithVariant(long id, Variant data) {
            }

            var rootType = writeRecordModel(RecordWithVariant.class)
                    .withField("id", LONG.notNull(), RecordWithVariant::id)
                    .withField("data", VARIANT, RecordWithVariant::data);

            String expected = """
                    message RecordWithVariant {
                      required int64 id;
                      optional group data (VARIANT(1)) {
                        required binary metadata;
                        required binary value;
                      }
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void recordWithNotNullVariant() {
            record RecordWithVariant(long id, @NotNull Variant data) {
            }

            var rootType = writeRecordModel(RecordWithVariant.class)
                    .withField("id", LONG.notNull(), RecordWithVariant::id)
                    .withField("data", VARIANT.notNull(), RecordWithVariant::data);

            String expected = """
                    message RecordWithVariant {
                      required int64 id;
                      required group data (VARIANT(1)) {
                        required binary metadata;
                        required binary value;
                      }
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }
    }

    @Nested
    class DecimalConfigurationGlobal {

        @Test
        void recordField() {
            record RecordFieldDecimal(BigDecimal value) {
            }

            var rootType = writeRecordModel(RecordFieldDecimal.class)
                    .withField("value", BIG_DECIMAL, RecordFieldDecimal::value);

            CarpetWriteConfiguration config = config()
                    .decimalConfig(decimalConfig().withPrecisionAndScale(20, 4))
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
                    .decimalConfig(decimalConfig().withPrecisionAndScale(9, 4))
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
                    .decimalConfig(decimalConfig().withPrecisionAndScale(18, 8))
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
                    .decimalConfig(decimalConfig().withPrecisionAndScale(20, 4))
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
                    .decimalConfig(decimalConfig().withPrecisionAndScale(20, 4))
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
                    .decimalConfig(decimalConfig().withPrecisionAndScale(20, 4))
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
                    .decimalConfig(decimalConfig().withPrecisionAndScale(20, 4))
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

    @Nested
    class DecimalConfigurationPerValue {

        @Test
        void recordField() {
            record RecordFieldDecimal(BigDecimal value) {
            }

            var rootType = writeRecordModel(RecordFieldDecimal.class)
                    .withField("value", BIG_DECIMAL.withPrecisionScale(20, 4), RecordFieldDecimal::value);

            String expected = """
                    message RecordFieldDecimal {
                      optional binary value (DECIMAL(20,4));
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void intPrecision() {
            record RecordFieldDecimal(BigDecimal value) {
            }

            var rootType = writeRecordModel(RecordFieldDecimal.class)
                    .withField("value", BIG_DECIMAL.withPrecisionScale(9, 4), RecordFieldDecimal::value);

            String expected = """
                    message RecordFieldDecimal {
                      optional int32 value (DECIMAL(9,4));
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void longPrecision() {
            record RecordFieldDecimal(BigDecimal value) {
            }

            var rootType = writeRecordModel(RecordFieldDecimal.class)
                    .withField("value", BIG_DECIMAL.withPrecisionScale(18, 8), RecordFieldDecimal::value);

            String expected = """
                    message RecordFieldDecimal {
                      optional int64 value (DECIMAL(18,8));
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void collectionValue() {
            record CollectionDecimalValue(List<BigDecimal> value) {
            }

            var rootType = writeRecordModel(CollectionDecimalValue.class)
                    .withField("value", LIST.ofType(BIG_DECIMAL.withPrecisionScale(20, 4)),
                            CollectionDecimalValue::value);

            String expected = """
                    message CollectionDecimalValue {
                      optional group value (LIST) {
                        repeated group list {
                          optional binary element (DECIMAL(20,4));
                        }
                      }
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void nestedCollectionValue() {
            record NestedCollectionDecimalValue(List<List<BigDecimal>> value) {
            }

            var rootType = writeRecordModel(NestedCollectionDecimalValue.class)
                    .withField("value", LIST.ofType(LIST.ofType(BIG_DECIMAL.withPrecisionScale(20, 4))),
                            NestedCollectionDecimalValue::value);

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
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void mapKeyAndValue() {
            record MapKeyValueDecimals(Map<BigDecimal, BigDecimal> value) {
            }

            var rootType = writeRecordModel(MapKeyValueDecimals.class)
                    .withField("value",
                            MAP.ofTypes(BIG_DECIMAL.withPrecisionScale(16, 2),
                                    BIG_DECIMAL.withPrecisionScale(28, 6)),
                            MapKeyValueDecimals::value);

            String expected = """
                    message MapKeyValueDecimals {
                      optional group value (MAP) {
                        repeated group key_value {
                          required int64 key (DECIMAL(16,2));
                          optional binary value (DECIMAL(28,6));
                        }
                      }
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void nestedMapKeyAndValue() {
            record NestedMapKeyValueDecimal(Map<BigDecimal, Map<BigDecimal, BigDecimal>> value) {
            }

            var rootType = writeRecordModel(NestedMapKeyValueDecimal.class)
                    .withField("value", MAP.ofTypes(BIG_DECIMAL.withPrecisionScale(16, 2),
                            MAP.ofTypes(
                                    BIG_DECIMAL.withPrecisionScale(20, 4),
                                    BIG_DECIMAL.withPrecisionScale(24, 6))),
                            NestedMapKeyValueDecimal::value);

            String expected = """
                    message NestedMapKeyValueDecimal {
                      optional group value (MAP) {
                        repeated group key_value {
                          required int64 key (DECIMAL(16,2));
                          optional group value (MAP) {
                            repeated group key_value {
                              required binary key (DECIMAL(20,4));
                              optional binary value (DECIMAL(24,6));
                            }
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
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
        void nestedStringCollection() {
            record SimpleTypeCollection(String id, List<String> values) {
            }

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(STRING), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.ONE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

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

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(STRING.asEnum()), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.ONE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

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

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(ENUM.ofType(Category.class)), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.ONE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

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

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(STRING.asJson()), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.ONE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

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

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(BINARY), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.ONE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

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

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(BINARY.asBson()), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.ONE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      repeated binary values (BSON);
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedGeometryCollection() {
            record SimpleTypeCollection(String id, List<@ParquetGeometry Binary> values) {
            }

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(BINARY.asParquetGeometry(null)), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.ONE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      repeated binary values (GEOMETRY);
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedGeographyCollection() {
            record SimpleTypeCollection(String id, List<@ParquetGeography Binary> values) {
            }

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(BINARY.asParquetGeography(null, null)),
                            SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.ONE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      repeated binary values (GEOGRAPHY);
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
        void nestedStringCollection() {
            record SimpleTypeCollection(String id, List<String> values) {
            }

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(STRING), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.TWO).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

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

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(STRING.asEnum()), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.TWO).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

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

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(ENUM.ofType(Category.class)), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.TWO).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

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

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(STRING.asJson()), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.TWO).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

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

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(BINARY), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.TWO).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

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

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(BINARY.asBson()), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.TWO).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

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
        void nestedGeometryCollection() {
            record SimpleTypeCollection(String id, List<@ParquetGeometry Binary> values) {
            }

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(BINARY.asParquetGeometry(null)), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.TWO).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated binary element (GEOMETRY);
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedGeographyCollection() {
            record SimpleTypeCollection(String id, List<@ParquetGeography Binary> values) {
            }

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(BINARY.asParquetGeography(null, null)),
                            SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.TWO).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated binary element (GEOGRAPHY);
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
        void nestedStringCollection() {
            record SimpleTypeCollection(String id, List<String> values) {
            }

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(STRING), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.THREE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

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

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(STRING.asEnum()), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.THREE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

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

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(ENUM.ofType(Category.class)), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.THREE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

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

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(STRING.asJson()), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.THREE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

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

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(BINARY), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.THREE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

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

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(BINARY.asBson()), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.THREE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

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
        void nestedGeometryCollection() {
            record SimpleTypeCollection(String id, List<@ParquetGeometry Binary> values) {
            }

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(BINARY.asParquetGeometry(null)), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.THREE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional binary element (GEOMETRY);
                        }
                      }
                    }
                    """;
            assertEquals(expected, schema.toString());
        }

        @Test
        void nestedGeographyCollection() {
            record SimpleTypeCollection(String id, List<@ParquetGeography Binary> values) {
            }

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING, SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(BINARY.asParquetGeography(null, null)),
                            SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.THREE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

            String expected = """
                    message SimpleTypeCollection {
                      optional binary id (STRING);
                      optional group values (LIST) {
                        repeated group list {
                          optional binary element (GEOGRAPHY);
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

            var rootType = writeRecordModel(SimpleTypeCollection.class)
                    .withField("id", STRING.notNull(), SimpleTypeCollection::id)
                    .withField("values", LIST.ofType(STRING.notNull()).notNull(), SimpleTypeCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.THREE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

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
        void consecutiveTripleNestedNotNullCollection() {

            record ConsecutiveTripleNestedNotNullCollection(String id, List<List<List<String>>> values) {
            }

            var rootType = writeRecordModel(ConsecutiveTripleNestedNotNullCollection.class)
                    .withField("id", STRING.notNull(), ConsecutiveTripleNestedNotNullCollection::id)
                    .withField("values",
                            LIST.notNull().ofType(LIST.notNull().ofType(LIST.notNull().ofType(STRING.notNull()))),
                            ConsecutiveTripleNestedNotNullCollection::values);

            var config = config().annotatedLevels(AnnotatedLevels.THREE).build();
            MessageType schema = new WriteRecordModel2Schema(config).createSchema(rootType);

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
        void nestedNotNullableTypeMap() {
            record SimpleTypeMap(String id, Map<String, Integer> values) {
            }

            var rootType = writeRecordModel(SimpleTypeMap.class)
                    .withField("id", STRING.notNull(), SimpleTypeMap::id)
                    .withField("values", MAP.ofTypes(STRING, INTEGER.notNull()).notNull(), SimpleTypeMap::values);

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
        void consecutiveTripleNestedNotNullMap() {

            record ConsecutiveTripleNestedNotNullMap(String id,
                    Map<String, Map<String, Map<String, String>>> values) {
            }

            var rootType = writeRecordModel(ConsecutiveTripleNestedNotNullMap.class)
                    .withField("id", STRING.notNull(), ConsecutiveTripleNestedNotNullMap::id)
                    .withField("values", MAP.notNull().ofTypes(STRING,
                            MAP.notNull().ofTypes(STRING, MAP.notNull().ofTypes(STRING, STRING.notNull()))),
                            ConsecutiveTripleNestedNotNullMap::values);

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

    @Nested
    class FieldIdMapping {

        @Test
        void simpleRecordWithFieldIds() {
            record SimpleRecord(
                    @FieldId(1) String uuid,
                    @FieldId(2) int statusCode,
                    @FieldId(3) long durationMillis,
                    @FieldId(4) String error) {
            }

            var rootType = writeRecordModel(SimpleRecord.class)
                    .withField("uuid", STRING.fieldId(1), SimpleRecord::uuid)
                    .withField("statusCode", INTEGER.notNull().fieldId(2), SimpleRecord::statusCode)
                    .withField("durationMillis", LONG.notNull().fieldId(3), SimpleRecord::durationMillis)
                    .withField("error", STRING.fieldId(4), SimpleRecord::error);

            String expected = """
                    message SimpleRecord {
                      optional binary uuid (STRING) = 1;
                      required int32 statusCode = 2;
                      required int64 durationMillis = 3;
                      optional binary error (STRING) = 4;
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void mixedRecordWithAndWithoutFieldIds() {
            record MixedRecord(
                    @FieldId(10) String id,
                    String name,
                    @FieldId(20) int value) {
            }

            var rootType = writeRecordModel(MixedRecord.class)
                    .withField("id", STRING.fieldId(10), MixedRecord::id)
                    .withField("name", STRING, MixedRecord::name)
                    .withField("value", INTEGER.notNull().fieldId(20), MixedRecord::value);

            String expected = """
                    message MixedRecord {
                      optional binary id (STRING) = 10;
                      optional binary name (STRING);
                      required int32 value = 20;
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void collectionWithFieldIds() {
            record CollectionRecord(@FieldId(1) String id, @FieldId(2) List<Integer> values) {
            }

            var rootType = writeRecordModel(CollectionRecord.class)
                    .withField("id", STRING.fieldId(1), CollectionRecord::id)
                    .withField("values", LIST.ofType(INTEGER).fieldId(2), CollectionRecord::values);

            String expected = """
                    message CollectionRecord {
                      optional binary id (STRING) = 1;
                      optional group values (LIST) = 2 {
                        repeated group list {
                          optional int32 element;
                        }
                      }
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void mapWithFieldIds() {
            record MapRecord(@FieldId(1) String id, @FieldId(2) Map<String, Integer> scores) {
            }

            var rootType = writeRecordModel(MapRecord.class)
                    .withField("id", STRING.fieldId(1), MapRecord::id)
                    .withField("scores", MAP.ofTypes(STRING, INTEGER).fieldId(2), MapRecord::scores);

            String expected = """
                    message MapRecord {
                      optional binary id (STRING) = 1;
                      optional group scores (MAP) = 2 {
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
        void nestedRecordWithFieldIds() {
            record ChildRecord(@FieldId(100) String key, @FieldId(101) int value) {
            }
            record ParentRecord(@FieldId(1) long id, @FieldId(2) String name, @FieldId(3) ChildRecord child) {
            }

            var childType = writeRecordModel(ChildRecord.class)
                    .withField("key", STRING.fieldId(100), ChildRecord::key)
                    .withField("value", INTEGER.notNull().fieldId(101), ChildRecord::value);

            var rootType = writeRecordModel(ParentRecord.class)
                    .withField("id", LONG.notNull().fieldId(1), ParentRecord::id)
                    .withField("name", STRING.fieldId(2), ParentRecord::name)
                    .withField("child", childType.fieldId(3), ParentRecord::child);

            String expected = """
                    message ParentRecord {
                      required int64 id = 1;
                      optional binary name (STRING) = 2;
                      optional group child = 3 {
                        optional binary key (STRING) = 100;
                        required int32 value = 101;
                      }
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void complexNestedStructureWithFieldIds() {
            record ChildRecord1(
                    @FieldId(11) String attribute,
                    @FieldId(12) long value) {
            }

            record ChildRecord2(
                    @FieldId(21) String attribute,
                    @FieldId(22) long value) {
            }

            record ChildRecord3(
                    @FieldId(31) String attribute,
                    @FieldId(32) long value) {
            }

            record ParentRecord(
                    @FieldId(1) String id,
                    @FieldId(2) int status,
                    @FieldId(3) long durationMillis,
                    @FieldId(4) String error,
                    @FieldId(5) ChildRecord1 foo,
                    @FieldId(6) List<ChildRecord2> bar,
                    @FieldId(7) Map<String, ChildRecord3> baz) {
            }

            var childType1 = writeRecordModel(ChildRecord1.class)
                    .withField("attribute", STRING.fieldId(11), ChildRecord1::attribute)
                    .withField("value", LONG.notNull().fieldId(12), ChildRecord1::value);

            var childType2 = writeRecordModel(ChildRecord2.class)
                    .withField("attribute", STRING.fieldId(21), ChildRecord2::attribute)
                    .withField("value", LONG.notNull().fieldId(22), ChildRecord2::value);

            var childType3 = writeRecordModel(ChildRecord3.class)
                    .withField("attribute", STRING.fieldId(31), ChildRecord3::attribute)
                    .withField("value", LONG.notNull().fieldId(32), ChildRecord3::value);

            var rootType = writeRecordModel(ParentRecord.class)
                    .withField("id", STRING.fieldId(1), ParentRecord::id)
                    .withField("status", INTEGER.notNull().fieldId(2), ParentRecord::status)
                    .withField("durationMillis", LONG.notNull().fieldId(3), ParentRecord::durationMillis)
                    .withField("error", STRING.fieldId(4), ParentRecord::error)
                    .withField("foo", childType1.fieldId(5), ParentRecord::foo)
                    .withField("bar", LIST.ofType(childType2).fieldId(6), ParentRecord::bar)
                    .withField("baz", MAP.ofTypes(STRING, childType3).fieldId(7), ParentRecord::baz);

            String expected = """
                    message ParentRecord {
                      optional binary id (STRING) = 1;
                      required int32 status = 2;
                      required int64 durationMillis = 3;
                      optional binary error (STRING) = 4;
                      optional group foo = 5 {
                        optional binary attribute (STRING) = 11;
                        required int64 value = 12;
                      }
                      optional group bar (LIST) = 6 {
                        repeated group list {
                          optional group element {
                            optional binary attribute (STRING) = 21;
                            required int64 value = 22;
                          }
                        }
                      }
                      optional group baz (MAP) = 7 {
                        repeated group key_value {
                          required binary key (STRING);
                          optional group value {
                            optional binary attribute (STRING) = 31;
                            required int64 value = 32;
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void nestedRecordsInListElementsShouldHaveFieldIds() {
            record Item(@FieldId(100) String name, @FieldId(101) int quantity) {
            }
            record TestRecord(@FieldId(1) List<Item> items) {
            }

            var itemType = writeRecordModel(Item.class)
                    .withField("name", STRING.fieldId(100), Item::name)
                    .withField("quantity", INTEGER.notNull().fieldId(101), Item::quantity);

            var rootType = writeRecordModel(TestRecord.class)
                    .withField("items", LIST.ofType(itemType).fieldId(1), TestRecord::items);

            String expected = """
                    message TestRecord {
                      optional group items (LIST) = 1 {
                        repeated group list {
                          optional group element {
                            optional binary name (STRING) = 100;
                            required int32 quantity = 101;
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void nestedRecordsInMapValuesShouldHaveFieldIds() {
            record Item(@FieldId(200) String description, @FieldId(201) double price) {
            }
            record TestRecord(@FieldId(1) Map<String, Item> products) {
            }

            var itemType = writeRecordModel(Item.class)
                    .withField("description", STRING.fieldId(200), Item::description)
                    .withField("price", DOUBLE.notNull().fieldId(201), Item::price);

            var rootType = writeRecordModel(TestRecord.class)
                    .withField("products", MAP.ofTypes(STRING, itemType).fieldId(1), TestRecord::products);

            String expected = """
                    message TestRecord {
                      optional group products (MAP) = 1 {
                        repeated group key_value {
                          required binary key (STRING);
                          optional group value {
                            optional binary description (STRING) = 200;
                            required double price = 201;
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void nestedCollectionsShouldHandleFieldIdsCorrectly() {
            record TestRecord(
                    @FieldId(1) List<List<String>> nestedLists,
                    @FieldId(2) Map<String, List<Integer>> mapWithLists) {
            }

            var rootType = writeRecordModel(TestRecord.class)
                    .withField("nestedLists", LIST.ofType(LIST.ofType(STRING)).fieldId(1), TestRecord::nestedLists)
                    .withField("mapWithLists", MAP.ofTypes(STRING, LIST.ofType(INTEGER)).fieldId(2),
                            TestRecord::mapWithLists);

            String expected = """
                    message TestRecord {
                      optional group nestedLists (LIST) = 1 {
                        repeated group list {
                          optional group element (LIST) {
                            repeated group list {
                              optional binary element (STRING);
                            }
                          }
                        }
                      }
                      optional group mapWithLists (MAP) = 2 {
                        repeated group key_value {
                          required binary key (STRING);
                          optional group value (LIST) {
                            repeated group list {
                              optional int32 element;
                            }
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void sameFieldIdsInDifferentRecordScopesShouldBeAllowed() {
            record ChildRecord(@FieldId(1) String attr) {
            }
            record ParentRecord(
                    @FieldId(1) String id,
                    @FieldId(2) ChildRecord child1,
                    @FieldId(3) Map<String, ChildRecord> child2) {
            }

            var childType = writeRecordModel(ChildRecord.class)
                    .withField("attr", STRING.fieldId(1), ChildRecord::attr);

            var rootType = writeRecordModel(ParentRecord.class)
                    .withField("id", STRING.fieldId(1), ParentRecord::id)
                    .withField("child1", childType.fieldId(2), ParentRecord::child1)
                    .withField("child2", MAP.ofTypes(STRING, childType).fieldId(3), ParentRecord::child2);

            String expected = """
                    message ParentRecord {
                      optional binary id (STRING) = 1;
                      optional group child1 = 2 {
                        optional binary attr (STRING) = 1;
                      }
                      optional group child2 (MAP) = 3 {
                        repeated group key_value {
                          required binary key (STRING);
                          optional group value {
                            optional binary attr (STRING) = 1;
                          }
                        }
                      }
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void setWithFieldIdsShouldWorkLikeList() {
            record TestRecord(@FieldId(1) Set<String> tags) {
            }

            var rootType = writeRecordModel(TestRecord.class)
                    .withField("tags", LIST.ofType(STRING).fieldId(1), TestRecord::tags);

            String expected = """
                    message TestRecord {
                      optional group tags (LIST) = 1 {
                        repeated group list {
                          optional binary element (STRING);
                        }
                      }
                    }
                    """;
            assertEquals(expected, schemaWithRootType(rootType).toString());
        }

        @Test
        void duplicateFieldIdsInSameRecordShouldThrowException() {
            record DuplicateIdRecord(
                    @FieldId(1) String field1,
                    @FieldId(1) String field2) {
            }

            var exception = assertThrows(RecordTypeConversionException.class,
                    () -> writeRecordModel(DuplicateIdRecord.class)
                            .withField("field1", STRING.fieldId(1), DuplicateIdRecord::field1)
                            .withField("field2", STRING.fieldId(1), DuplicateIdRecord::field2));

            assertTrue(exception.getMessage().contains("Duplicate field ID 1"));
            assertTrue(exception.getMessage().contains("DuplicateIdRecord"));
            assertTrue(exception.getMessage().contains("must be unique within the same record scope"));
        }

        @Test
        void multipleDuplicateFieldIdsShouldReportFirst() {
            record MultipleDuplicates(
                    @FieldId(1) String field1,
                    @FieldId(2) String field2,
                    @FieldId(1) String field3,
                    @FieldId(2) String field4) {
            }

            var exception = assertThrows(RecordTypeConversionException.class,
                    () -> writeRecordModel(MultipleDuplicates.class)
                            .withField("field1", STRING.fieldId(1), MultipleDuplicates::field1)
                            .withField("field2", STRING.fieldId(2), MultipleDuplicates::field2)
                            .withField("field3", STRING.fieldId(1), MultipleDuplicates::field3)
                            .withField("field4", STRING.fieldId(2), MultipleDuplicates::field4));

            // Should report the first duplicate encountered (field ID 1)
            assertTrue(exception.getMessage().contains("Duplicate field ID 1"));
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
