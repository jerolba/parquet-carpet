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
package com.jerolba.carpet.reader;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.generic.GenericData.Record;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.jerolba.carpet.CarpetParquetReader;
import com.jerolba.carpet.ParquetReaderTest;
import com.jerolba.carpet.filestream.FileSystemInputFile;
import com.jerolba.carpet.filestream.FileSystemOutputFile;
import com.jerolba.carpet.reader.CarpetReaderTest.Category;

class CarpetReaderConversionTest {

    @Nested
    class FieldConversion {

        @Nested
        class ToDouble {

            record ToDoubleConversion(double primitive, Double object) {
            }

            @Test
            void fromDoubleToDouble() throws IOException {
                var readerTest = fromDouble("FromDoubleToDoubleConversion");
                try (var carpetReader = readerTest.getCarpetReader(ToDoubleConversion.class)) {
                    assertEquals(new ToDoubleConversion(1.0, 2.0), carpetReader.read());
                    assertEquals(new ToDoubleConversion(3.0, null), carpetReader.read());
                }
            }

            @Test
            void fromFloatToDouble() throws IOException {
                var readerTest = fromFloat("FromFloatToDoubleConversion");
                try (var carpetReader = readerTest.getCarpetReader(ToDoubleConversion.class)) {
                    assertEquals(new ToDoubleConversion(1.0, 2.0), carpetReader.read());
                    assertEquals(new ToDoubleConversion(3.0, null), carpetReader.read());
                }
            }

            @Test
            void fromIntegerToDouble() throws IOException {
                var readerTest = fromInteger("FromIntegerToDoubleConversion");
                try (var carpetReader = readerTest.getCarpetReader(ToDoubleConversion.class)) {
                    assertEquals(new ToDoubleConversion(1.0, 2.0), carpetReader.read());
                    assertEquals(new ToDoubleConversion(3.0, null), carpetReader.read());
                }
            }

            @Test
            void fromLongToDouble() throws IOException {
                var readerTest = fromLong("FromLongToDoubleConversion");
                try (var carpetReader = readerTest.getCarpetReader(ToDoubleConversion.class)) {
                    assertEquals(new ToDoubleConversion(1.0, 2.0), carpetReader.read());
                    assertEquals(new ToDoubleConversion(3.0, null), carpetReader.read());
                }
            }

        }

        @Nested
        class ToFloat {

            record ToFloatConversion(float primitive, Float object) {
            }

            @Test
            void fromDoubleToFloat() throws IOException {
                var readerTest = fromDouble("FromDoubleToFloatConversion");
                try (var carpetReader = readerTest.getCarpetReader(ToFloatConversion.class)) {
                    assertEquals(new ToFloatConversion(1.0f, 2.0f), carpetReader.read());
                    assertEquals(new ToFloatConversion(3.0f, null), carpetReader.read());
                }
            }

            @Test
            void fromFloatToFloat() throws IOException {
                var readerTest = fromFloat("FromFloatToFloatConversion");
                try (var carpetReader = readerTest.getCarpetReader(ToFloatConversion.class)) {
                    assertEquals(new ToFloatConversion(1.0f, 2.0f), carpetReader.read());
                    assertEquals(new ToFloatConversion(3.0f, null), carpetReader.read());
                }
            }

            @Test
            void fromIntegerToFloat() throws IOException {
                var readerTest = fromInteger("FromIntegerToFloatConversion");
                try (var carpetReader = readerTest.getCarpetReader(ToFloatConversion.class)) {
                    assertEquals(new ToFloatConversion(1.0f, 2.0f), carpetReader.read());
                    assertEquals(new ToFloatConversion(3.0f, null), carpetReader.read());
                }
            }

            @Test
            void fromLongToFloat() throws IOException {
                var readerTest = fromLong("FromLongToFloatConversion");
                try (var carpetReader = readerTest.getCarpetReader(ToFloatConversion.class)) {
                    assertEquals(new ToFloatConversion(1.0f, 2.0f), carpetReader.read());
                    assertEquals(new ToFloatConversion(3.0f, null), carpetReader.read());
                }
            }
        }

        @Nested
        class ToLong {

            record ToLongConversion(long primitive, Long object) {
            }

            @Test
            void fromIntegerToLong() throws IOException {
                var readerTest = fromInteger("FromIntegerToLongConversion");
                try (var carpetReader = readerTest.getCarpetReader(ToLongConversion.class)) {
                    assertEquals(new ToLongConversion(1L, 2L), carpetReader.read());
                    assertEquals(new ToLongConversion(3L, null), carpetReader.read());
                }
            }

            @Test
            void fromLongToLong() throws IOException {
                var readerTest = fromLong("FromLongToLongConversion");
                try (var carpetReader = readerTest.getCarpetReader(ToLongConversion.class)) {
                    assertEquals(new ToLongConversion(1L, 2L), carpetReader.read());
                    assertEquals(new ToLongConversion(3L, null), carpetReader.read());
                }
            }

        }

        @Nested
        class ToInteger {

            record ToIntegerConversion(int primitive, Integer object) {
            }

            @Test
            void fromIntegerToInteger() throws IOException {
                var readerTest = fromInteger("FromIntegerToIntegerConversion");
                try (var carpetReader = readerTest.getCarpetReader(ToIntegerConversion.class)) {
                    assertEquals(new ToIntegerConversion(1, 2), carpetReader.read());
                    assertEquals(new ToIntegerConversion(3, null), carpetReader.read());
                }
            }

            @Test
            void fromLongToInteger() throws IOException {
                var readerTest = fromLong("FromLongToIntegerConversion");
                try (var carpetReader = readerTest.getCarpetReader(ToIntegerConversion.class)) {
                    assertEquals(new ToIntegerConversion(1, 2), carpetReader.read());
                    assertEquals(new ToIntegerConversion(3, null), carpetReader.read());
                }
            }

        }

        @Nested
        class ToShort {

            record ToShortConversion(short primitive, Short object) {
            }

            @Test
            void fromIntegerToShort() throws IOException {
                var readerTest = fromInteger("FromIntegerToShortConversion");
                try (var carpetReader = readerTest.getCarpetReader(ToShortConversion.class)) {
                    assertEquals(new ToShortConversion((short) 1, (short) 2), carpetReader.read());
                    assertEquals(new ToShortConversion((short) 3, null), carpetReader.read());
                }
            }

            @Test
            void fromLongToShort() throws IOException {
                var readerTest = fromLong("FromLongToShortConversion");
                try (var carpetReader = readerTest.getCarpetReader(ToShortConversion.class)) {
                    assertEquals(new ToShortConversion((short) 1, (short) 2), carpetReader.read());
                    assertEquals(new ToShortConversion((short) 3, null), carpetReader.read());
                }
            }

        }

        @Nested
        class ToByte {

            record ToByteConversion(byte primitive, Byte object) {
            }

            @Test
            void fromIntegerToByte() throws IOException {
                var readerTest = fromInteger("FromIntegerToByteConversion");
                try (var carpetReader = readerTest.getCarpetReader(ToByteConversion.class)) {
                    assertEquals(new ToByteConversion((byte) 1, (byte) 2), carpetReader.read());
                    assertEquals(new ToByteConversion((byte) 3, null), carpetReader.read());
                }
            }

            @Test
            void fromLongToByte() throws IOException {
                var readerTest = fromLong("FromLongToByteConversion");
                try (var carpetReader = readerTest.getCarpetReader(ToByteConversion.class)) {
                    assertEquals(new ToByteConversion((byte) 1, (byte) 2), carpetReader.read());
                    assertEquals(new ToByteConversion((byte) 3, null), carpetReader.read());
                }
            }

        }

        @Nested
        class ToString {

            record ToStringConversion(String value) {
            }

            @Test
            void fromStringToString() throws IOException {
                Schema schema = schemaType("FromStringToString")
                        .optionalString("value")
                        .endRecord();

                var readerTest = new ParquetReaderTest(schema);
                readerTest.writer(writer -> {
                    Record record = new Record(schema);
                    record.put("value", "one");
                    writer.write(record);
                    record = new Record(schema);
                    record.put("value", null);
                    writer.write(record);
                });
                try (var carpetReader = readerTest.getCarpetReader(ToStringConversion.class)) {
                    assertEquals(new ToStringConversion("one"), carpetReader.read());
                    assertEquals(new ToStringConversion(null), carpetReader.read());
                }
            }

            @Test
            void fromEnumToString() throws IOException {
                Schema schema = SchemaBuilder.builder().record("FromEnumToString").fields()
                        .name("value").type().nullable().enumeration("Category")
                        .symbols("one", "two", "three").noDefault()
                        .endRecord();

                var readerTest = new ParquetReaderTest(schema);
                readerTest.writer(writer -> {
                    Record record = new Record(schema);
                    record.put("value", "one");
                    writer.write(record);
                    record = new Record(schema);
                    record.put("value", null);
                    writer.write(record);
                });
                try (var carpetReader = readerTest.getCarpetReader(ToStringConversion.class)) {
                    assertEquals(new ToStringConversion("one"), carpetReader.read());
                    assertEquals(new ToStringConversion(null), carpetReader.read());
                }
            }

            @Test
            void fromUuidToString() throws IOException {
                var uuid = UUID.randomUUID();
                File file = fromUuid("FromUuidToString", uuid);

                InputFile inputFile = new FileSystemInputFile(file);
                try (var carpetReader = CarpetParquetReader.builder(inputFile, ToStringConversion.class).build()) {
                    assertEquals(new ToStringConversion(uuid.toString()), carpetReader.read());
                    assertEquals(new ToStringConversion(null), carpetReader.read());
                }
            }

        }

        @Nested
        class ToEnum {

            record ToEnumConversion(Category value) {
            }

            @Test
            void fromStringToEnum() throws IOException {
                Schema schema = schemaType("FromStringToEnum")
                        .optionalString("value")
                        .endRecord();

                var readerTest = new ParquetReaderTest(schema);
                readerTest.writer(writer -> {
                    Record record = new Record(schema);
                    record.put("value", "one");
                    writer.write(record);
                    record = new Record(schema);
                    record.put("value", null);
                    writer.write(record);
                });
                try (var carpetReader = readerTest.getCarpetReader(ToEnumConversion.class)) {
                    assertEquals(new ToEnumConversion(Category.one), carpetReader.read());
                    assertEquals(new ToEnumConversion(null), carpetReader.read());
                }
            }

            @Test
            void fromEnumToEnum() throws IOException {
                Schema schema = SchemaBuilder.builder().record("FromEnumToEnum").fields()
                        .name("value").type().nullable().enumeration("Category")
                        .symbols("one", "two", "three").noDefault()
                        .endRecord();

                var readerTest = new ParquetReaderTest(schema);
                readerTest.writer(writer -> {
                    Record record = new Record(schema);
                    record.put("value", "one");
                    writer.write(record);
                    record = new Record(schema);
                    record.put("value", null);
                    writer.write(record);
                });
                try (var carpetReader = readerTest.getCarpetReader(ToEnumConversion.class)) {
                    assertEquals(new ToEnumConversion(Category.one), carpetReader.read());
                    assertEquals(new ToEnumConversion(null), carpetReader.read());
                }
            }

        }

        @Nested
        class ToUuid {

            @Test
            void fromUuidToUuid() throws IOException {
                var uuid = UUID.randomUUID();
                File file = fromUuid("FromUuidToUuid", uuid);

                record UuidType(UUID value) {
                }

                InputFile inputFile = new FileSystemInputFile(file);
                try (var carpetReader = CarpetParquetReader.builder(inputFile, UuidType.class).build()) {
                    assertEquals(new UuidType(uuid), carpetReader.read());
                    assertEquals(new UuidType(null), carpetReader.read());
                }
            }

        }

        private ParquetReaderTest fromDouble(String name) throws IOException {
            Schema schema = schemaType(name)
                    .requiredDouble("primitive")
                    .optionalDouble("object")
                    .endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("primitive", 1.0);
                record.put("object", 2.0);
                writer.write(record);
                record = new Record(schema);
                record.put("primitive", 3.0);
                record.put("object", null);
                writer.write(record);
            });
            return readerTest;
        }

        private ParquetReaderTest fromFloat(String name) throws IOException {
            Schema schema = schemaType(name)
                    .requiredFloat("primitive")
                    .optionalFloat("object")
                    .endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("primitive", 1.0f);
                record.put("object", 2.0f);
                writer.write(record);
                record = new Record(schema);
                record.put("primitive", 3.0f);
                record.put("object", null);
                writer.write(record);
            });
            return readerTest;
        }

        private ParquetReaderTest fromInteger(String name) throws IOException {
            Schema schema = schemaType(name)
                    .requiredInt("primitive")
                    .optionalInt("object")
                    .endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("primitive", 1);
                record.put("object", 2);
                writer.write(record);
                record = new Record(schema);
                record.put("primitive", 3);
                record.put("object", null);
                writer.write(record);
            });
            return readerTest;
        }

        private ParquetReaderTest fromLong(String name) throws IOException {
            Schema schema = schemaType(name)
                    .requiredLong("primitive")
                    .optionalLong("object")
                    .endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("primitive", 1L);
                record.put("object", 2L);
                writer.write(record);
                record = new Record(schema);
                record.put("primitive", 3L);
                record.put("object", null);
                writer.write(record);
            });
            return readerTest;
        }

        private File fromUuid(String name, UUID uuid) throws IOException {
            Schema schema = SchemaBuilder.builder().record("UUID").fields()
                    .name("value").type().nullable().stringType().noDefault()
                    .endRecord();
            var valueSchema = schema.getField("value").schema().getTypes().stream()
                    .filter(t -> !t.getType().equals(Schema.Type.NULL)).findFirst().get();
            LogicalTypes.uuid().addToSchema(valueSchema);

            File file = new File(ParquetReaderTest.getTestFilePath(name));
            OutputFile output = new FileSystemOutputFile(file);
            try (ParquetWriter<Record> writer = AvroParquetWriter.<Record>builder(output)
                    .config(AvroWriteSupport.WRITE_PARQUET_UUID, "true")
                    .withSchema(schema)
                    .build()) {
                Record record = new Record(schema);
                record.put("value", uuid);
                writer.write(record);
                record = new Record(schema);
                record.put("value", null);
                writer.write(record);
            }
            return file;
        }
    }

    @Nested
    class ConversionInCollection {

        @Nested
        class ToIntegerList {

            record IntList(List<Integer> value) {
            }

            @Test
            void fromIntToIntList() throws IOException {
                var readerTest = fromIntList("FromIntToIntList", 1, 2, 3);
                try (var carpetReader = readerTest.getCarpetReader(IntList.class)) {
                    assertEquals(new IntList(List.of(1, 2, 3)), carpetReader.read());
                }
            }

            @Test
            void fromLongToIntList() throws IOException {
                var readerTest = fromLongList("FromLongToIntList", 1L, 2L, 3L);
                try (var carpetReader = readerTest.getCarpetReader(IntList.class)) {
                    assertEquals(new IntList(List.of(1, 2, 3)), carpetReader.read());
                }
            }
        }

        @Nested
        class ToLongList {

            record LongList(List<Long> value) {
            }

            @Test
            void fromLongToLongList() throws IOException {
                var readerTest = fromLongList("FromLongToLongList", 1L, 2L, 3L);
                try (var carpetReader = readerTest.getCarpetReader(LongList.class)) {
                    assertEquals(new LongList(List.of(1L, 2L, 3L)), carpetReader.read());
                }
            }

            @Test
            void fromIntToLongList() throws IOException {
                var readerTest = fromIntList("FromIntToLongList", 1, 2, 3);
                try (var carpetReader = readerTest.getCarpetReader(LongList.class)) {
                    assertEquals(new LongList(List.of(1L, 2L, 3L)), carpetReader.read());
                }
            }
        }

        @Nested
        class ToDoubleList {

            record DoubleList(List<Double> value) {
            }

            @Test
            void fromDoubleToDoubleList() throws IOException {
                var readerTest = fromDoubleList("FromDoubleToDoubleList", 1.9, 2.9, 3.9);
                try (var carpetReader = readerTest.getCarpetReader(DoubleList.class)) {
                    assertEquals(new DoubleList(List.of(1.9, 2.9, 3.9)), carpetReader.read());
                }
            }

            @Test
            void fromFloatToDoubleList() throws IOException {
                var readerTest = fromFloatList("FromFloatToDoubleList", 1.9f, 2.9f, 3.9f);
                try (var carpetReader = readerTest.getCarpetReader(DoubleList.class)) {
                    DoubleList list = carpetReader.read();
                    assertEquals(1.9f, list.value.get(0), 0.0001);
                    assertEquals(2.9f, list.value.get(1), 0.0001);
                    assertEquals(3.9f, list.value.get(2), 0.0001);
                }
            }

            @Test
            void fromIntegerToDoubleList() throws IOException {
                var readerTest = fromIntList("FromIntegerToDoubleList", 1, 2, 3);
                try (var carpetReader = readerTest.getCarpetReader(DoubleList.class)) {
                    assertEquals(new DoubleList(List.of(1.0, 2.0, 3.0)), carpetReader.read());
                }
            }

            @Test
            void fromLongToDoubleList() throws IOException {
                var readerTest = fromLongList("FromLongToDoubleList", 1L, 2L, 3L);
                try (var carpetReader = readerTest.getCarpetReader(DoubleList.class)) {
                    assertEquals(new DoubleList(List.of(1.0, 2.0, 3.0)), carpetReader.read());
                }
            }

        }

        @Nested
        class ToFloatList {

            record FloatList(List<Float> value) {
            }

            @Test
            void fromFloatToFloatList() throws IOException {
                var readerTest = fromFloatList("FromFloatToFloatList", 1.9f, 2.9f, 3.9f);
                try (var carpetReader = readerTest.getCarpetReader(FloatList.class)) {
                    assertEquals(new FloatList(List.of(1.9f, 2.9f, 3.9f)), carpetReader.read());
                }
            }

            @Test
            void fromDoubleToFloatList() throws IOException {
                var readerTest = fromDoubleList("FromDoubleToFloatList", 1.9, 2.9, 3.9);
                try (var carpetReader = readerTest.getCarpetReader(FloatList.class)) {
                    assertEquals(new FloatList(List.of(1.9f, 2.9f, 3.9f)), carpetReader.read());
                }
            }

            @Test
            void fromIntegerToFloatList() throws IOException {
                var readerTest = fromIntList("FromIntegerToFloatList", 1, 2, 3);
                try (var carpetReader = readerTest.getCarpetReader(FloatList.class)) {
                    assertEquals(new FloatList(List.of(1.0f, 2.0f, 3.0f)), carpetReader.read());
                }
            }

            @Test
            void fromLongToFloatList() throws IOException {
                var readerTest = fromLongList("FromLongToFloatList", 1L, 2L, 3L);
                try (var carpetReader = readerTest.getCarpetReader(FloatList.class)) {
                    assertEquals(new FloatList(List.of(1.0f, 2.0f, 3.0f)), carpetReader.read());
                }
            }

        }

        @Nested
        class ToShortList {

            record ShortList(List<Short> value) {
            }

            @Test
            void fromIntegerToShortList() throws IOException {
                var readerTest = fromIntList("FromIntegerToShortList", 1, 2, 3);
                try (var carpetReader = readerTest.getCarpetReader(ShortList.class)) {
                    assertEquals(new ShortList(List.of((short) 1, (short) 2, (short) 3)), carpetReader.read());
                }
            }

            @Test
            void fromLongToShortList() throws IOException {
                var readerTest = fromLongList("FromLongToShortList", 1L, 2L, 3L);
                try (var carpetReader = readerTest.getCarpetReader(ShortList.class)) {
                    assertEquals(new ShortList(List.of((short) 1, (short) 2, (short) 3)), carpetReader.read());
                }
            }
        }

        @Nested
        class ToByteList {

            record ByteList(List<Byte> value) {
            }

            @Test
            void fromIntegerToByteList() throws IOException {
                var readerTest = fromIntList("FromIntegerToByteList", 1, 2, 3);
                try (var carpetReader = readerTest.getCarpetReader(ByteList.class)) {
                    assertEquals(new ByteList(List.of((byte) 1, (byte) 2, (byte) 3)), carpetReader.read());
                }
            }

            @Test
            void fromLongToByteList() throws IOException {
                var readerTest = fromLongList("FromLongToByteList", 1L, 2L, 3L);
                try (var carpetReader = readerTest.getCarpetReader(ByteList.class)) {
                    assertEquals(new ByteList(List.of((byte) 1, (byte) 2, (byte) 3)), carpetReader.read());
                }
            }
        }

        @Test
        void booleanList() throws IOException {
            Schema schema = SchemaBuilder.builder().record("BooleanList").fields()
                    .name("value").type().array().items(Schema.create(Type.BOOLEAN)).noDefault()
                    .endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", List.of(true, false, true));
                writer.write(record);
            });

            record BooleanList(List<Boolean> value) {
            }

            try (var carpetReader = readerTest.getCarpetReader(BooleanList.class)) {
                assertEquals(new BooleanList(List.of(true, false, true)), carpetReader.read());
            }
        }

        @Nested
        class ToStringList {

            record StringList(List<String> value) {
            }

            @Test
            void fromStringToStringList() throws IOException {
                Schema schema = SchemaBuilder.builder().record("FromStringToStringList").fields()
                        .name("value").type().array().items(Schema.create(Type.STRING)).noDefault()
                        .endRecord();

                var readerTest = new ParquetReaderTest(schema);
                readerTest.writer(writer -> {
                    Record record = new Record(schema);
                    record.put("value", List.of("foo", "bar"));
                    writer.write(record);
                });

                try (var carpetReader = readerTest.getCarpetReader(StringList.class)) {
                    assertEquals(new StringList(List.of("foo", "bar")), carpetReader.read());
                }
            }

            @Test
            void fromEnumToStringList() throws IOException {
                Schema enumType = Schema.createEnum("Cetegory", null, null, List.of("one", "two", "three"));
                Schema schema = SchemaBuilder.builder().record("FromEnumToStringList").fields()
                        .name("value").type().array().items(enumType).noDefault()
                        .endRecord();

                var readerTest = new ParquetReaderTest(schema);
                readerTest.writer(writer -> {
                    Record record = new Record(schema);
                    record.put("value", List.of("foo", "bar"));
                    writer.write(record);
                });

                try (var carpetReader = readerTest.getCarpetReader(StringList.class)) {
                    assertEquals(new StringList(List.of("foo", "bar")), carpetReader.read());
                }
            }

            @Test
            void fromUuidToStringList() throws IOException {
                UUID uuid1 = UUID.randomUUID();
                UUID uuid2 = UUID.randomUUID();
                var file = fromUuidList("FromUuidToStringList", uuid1, uuid2);
                InputFile inputFile = new FileSystemInputFile(file);
                try (var carpetReader = CarpetParquetReader.builder(inputFile, StringList.class).build()) {
                    assertEquals(new StringList(List.of(uuid1.toString(), uuid2.toString())), carpetReader.read());
                }
            }

        }

        @Nested
        class ToUuidList {

            record UuidList(List<UUID> value) {
            }

            @Test
            void fromUuidToUuidList() throws IOException {
                UUID uuid1 = UUID.randomUUID();
                UUID uuid2 = UUID.randomUUID();
                var file = fromUuidList("FromUuidToUuidList", uuid1, uuid2);
                InputFile inputFile = new FileSystemInputFile(file);
                try (var carpetReader = CarpetParquetReader.builder(inputFile, UuidList.class).build()) {
                    assertEquals(new UuidList(List.of(uuid1, uuid2)), carpetReader.read());
                }
            }

        }

        @Nested
        class ToEnumList {

            record EnumList(List<Category> value) {
            }

            @Test
            void fromEnumToEnumList() throws IOException {
                Schema enumType = Schema.createEnum("Cetegory", null, null, List.of("one", "two", "three"));
                Schema schema = SchemaBuilder.builder().record("StringList").fields()
                        .name("value").type().array().items(enumType).noDefault()
                        .endRecord();

                var readerTest = new ParquetReaderTest(schema);
                readerTest.writer(writer -> {
                    Record record = new Record(schema);
                    record.put("value", List.of("one", "two"));
                    writer.write(record);
                });

                try (var carpetReader = readerTest.getCarpetReader(EnumList.class)) {
                    assertEquals(new EnumList(List.of(Category.one, Category.two)), carpetReader.read());
                }
            }

            @Test
            void fromStringToEnumList() throws IOException {
                Schema schema = SchemaBuilder.builder().record("FromStringToEnumList").fields()
                        .name("value").type().array().items(Schema.create(Type.STRING)).noDefault()
                        .endRecord();

                var readerTest = new ParquetReaderTest(schema);
                readerTest.writer(writer -> {
                    Record record = new Record(schema);
                    record.put("value", List.of("one", "two"));
                    writer.write(record);
                });

                try (var carpetReader = readerTest.getCarpetReader(EnumList.class)) {
                    assertEquals(new EnumList(List.of(Category.one, Category.two)), carpetReader.read());
                }
            }

        }

        private ParquetReaderTest fromIntList(String name, Integer... values) throws IOException {
            Schema schema = SchemaBuilder.builder().record(name).fields()
                    .name("value").type().array().items(Schema.create(Type.INT)).noDefault()
                    .endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", List.of(values));
                writer.write(record);
            });
            return readerTest;
        }

        private ParquetReaderTest fromLongList(String name, Long... values) throws IOException {
            Schema schema = SchemaBuilder.builder().record(name).fields()
                    .name("value").type().array().items(Schema.create(Type.LONG)).noDefault()
                    .endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", List.of(values));
                writer.write(record);
            });
            return readerTest;
        }

        private ParquetReaderTest fromDoubleList(String name, Double... values) throws IOException {
            Schema schema = SchemaBuilder.builder().record(name).fields()
                    .name("value").type().array().items(Schema.create(Type.DOUBLE)).noDefault()
                    .endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", List.of(values));
                writer.write(record);
            });
            return readerTest;
        }

        private ParquetReaderTest fromFloatList(String name, Float... values) throws IOException {
            Schema schema = SchemaBuilder.builder().record(name).fields()
                    .name("value").type().array().items(Schema.create(Type.FLOAT)).noDefault()
                    .endRecord();

            var readerTest = new ParquetReaderTest(schema);
            readerTest.writer(writer -> {
                Record record = new Record(schema);
                record.put("value", List.of(values));
                writer.write(record);
            });
            return readerTest;
        }

        private File fromUuidList(String name, UUID... values) throws IOException {
            Schema uuidSchema = Schema.create(Type.STRING);
            LogicalTypes.uuid().addToSchema(uuidSchema);

            Schema schema = SchemaBuilder.builder().record("UUID").fields()
                    .name("value").type().array().items(uuidSchema).noDefault()
                    .endRecord();

            File file = new File(ParquetReaderTest.getTestFilePath(name));
            OutputFile output = new FileSystemOutputFile(file);
            try (ParquetWriter<Record> writer = AvroParquetWriter.<Record>builder(output)
                    .config(AvroWriteSupport.WRITE_PARQUET_UUID, "true")
                    .withSchema(schema)
                    .build()) {
                Record record = new Record(schema);
                record.put("value", List.of(values));
                writer.write(record);
            }
            return file;
        }
    }

    private static FieldAssembler<Schema> schemaType(String type) {
        return SchemaBuilder.builder().record(type).fields();
    }
}
