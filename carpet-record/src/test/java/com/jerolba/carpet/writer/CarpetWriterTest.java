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

import static com.jerolba.carpet.ColumnNamingStrategy.SNAKE_CASE;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.jerolba.carpet.CarpetReader;
import com.jerolba.carpet.CarpetWriter;
import com.jerolba.carpet.FieldMatchingStrategy;
import com.jerolba.carpet.ParquetWriterTest;
import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.annotation.Alias;
import com.jerolba.carpet.io.FileSystemInputFile;
import com.jerolba.carpet.io.FileSystemOutputFile;

class CarpetWriterTest {

    enum Category {
        one, two, three;
    }

    @Nested
    class SimpleTypes {

        @Test
        void intPrimitive() throws IOException {

            record IntPrimitive(int value) {
            }

            var rec1 = new IntPrimitive(1);
            var rec2 = new IntPrimitive(2);
            var writerTest = new ParquetWriterTest<>(IntPrimitive.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.value, avroReader.read().get("value"));
            assertEquals(rec2.value, avroReader.read().get("value"));

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void longPrimitive() throws IOException {

            record LongPrimitive(long value) {
            }

            var rec1 = new LongPrimitive(191919191919L);
            var rec2 = new LongPrimitive(292929292929L);
            var writerTest = new ParquetWriterTest<>(LongPrimitive.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.value, avroReader.read().get("value"));
            assertEquals(rec2.value, avroReader.read().get("value"));

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void doublePrimitive() throws IOException {

            record DoublePrimitive(double value) {
            }

            var rec1 = new DoublePrimitive(1.9);
            var rec2 = new DoublePrimitive(2.9);
            var writerTest = new ParquetWriterTest<>(DoublePrimitive.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.value, avroReader.read().get("value"));
            assertEquals(rec2.value, avroReader.read().get("value"));

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void floatPrimitive() throws IOException {

            record FloatPrimitive(float value) {
            }

            var rec1 = new FloatPrimitive(1.9f);
            var rec2 = new FloatPrimitive(2.9f);
            var writerTest = new ParquetWriterTest<>(FloatPrimitive.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.value, avroReader.read().get("value"));
            assertEquals(rec2.value, avroReader.read().get("value"));

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void shortPrimitive() throws IOException {

            record ShortPrimitive(short value) {
            }

            var rec1 = new ShortPrimitive((short) 1);
            var rec2 = new ShortPrimitive((short) 2);
            var writerTest = new ParquetWriterTest<>(ShortPrimitive.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.value, ((Number) avroReader.read().get("value")).shortValue());
            assertEquals(rec2.value, ((Number) avroReader.read().get("value")).shortValue());

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void bytePrimitive() throws IOException {

            record BytePrimitive(byte value) {
            }

            var rec1 = new BytePrimitive((byte) 1);
            var rec2 = new BytePrimitive((byte) 2);
            var writerTest = new ParquetWriterTest<>(BytePrimitive.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.value, ((Number) avroReader.read().get("value")).byteValue());
            assertEquals(rec2.value, ((Number) avroReader.read().get("value")).byteValue());

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void booleanPrimitive() throws IOException {

            record BooleanPrimitive(boolean value) {
            }

            var rec1 = new BooleanPrimitive(true);
            var rec2 = new BooleanPrimitive(false);
            var writerTest = new ParquetWriterTest<>(BooleanPrimitive.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.value, avroReader.read().get("value"));
            assertEquals(rec2.value, avroReader.read().get("value"));

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void integerObject() throws IOException {

            record IntegerObject(Integer value) {
            }

            var rec1 = new IntegerObject(1);
            var rec2 = new IntegerObject(2);
            var writerTest = new ParquetWriterTest<>(IntegerObject.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.value, avroReader.read().get("value"));
            assertEquals(rec2.value, avroReader.read().get("value"));

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void longObject() throws IOException {

            record LongObject(Long value) {
            }

            var rec1 = new LongObject(191919191919L);
            var rec2 = new LongObject(292929292929L);
            var writerTest = new ParquetWriterTest<>(LongObject.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.value, avroReader.read().get("value"));
            assertEquals(rec2.value, avroReader.read().get("value"));

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void doubleObject() throws IOException {

            record DoubleObject(Double value) {
            }

            var rec1 = new DoubleObject(1.9);
            var rec2 = new DoubleObject(2.9);
            var writerTest = new ParquetWriterTest<>(DoubleObject.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.value, avroReader.read().get("value"));
            assertEquals(rec2.value, avroReader.read().get("value"));

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void floatObject() throws IOException {

            record FloatObject(Float value) {
            }

            var rec1 = new FloatObject(1.9f);
            var rec2 = new FloatObject(2.9f);
            var writerTest = new ParquetWriterTest<>(FloatObject.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.value, avroReader.read().get("value"));
            assertEquals(rec2.value, avroReader.read().get("value"));

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void shortObject() throws IOException {

            record ShortObject(Short value) {
            }

            var rec1 = new ShortObject((short) 1);
            var rec2 = new ShortObject((short) 2);
            var writerTest = new ParquetWriterTest<>(ShortObject.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.value, ((Number) avroReader.read().get("value")).shortValue());
            assertEquals(rec2.value, ((Number) avroReader.read().get("value")).shortValue());

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void byteObject() throws IOException {

            record ByteObject(Byte value) {
            }

            var rec1 = new ByteObject((byte) 1);
            var rec2 = new ByteObject((byte) 2);
            var writerTest = new ParquetWriterTest<>(ByteObject.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.value, ((Number) avroReader.read().get("value")).byteValue());
            assertEquals(rec2.value, ((Number) avroReader.read().get("value")).byteValue());

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void booleanObject() throws IOException {

            record BooleanObject(Boolean value) {
            }

            var rec1 = new BooleanObject(true);
            var rec2 = new BooleanObject(false);
            var writerTest = new ParquetWriterTest<>(BooleanObject.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.value, avroReader.read().get("value"));
            assertEquals(rec2.value, avroReader.read().get("value"));

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void stringObject() throws IOException {

            record StringObject(String value) {
            }

            var rec1 = new StringObject("Madrid");
            var rec2 = new StringObject("Zaragoza");
            var writerTest = new ParquetWriterTest<>(StringObject.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.value, avroReader.read().get("value").toString());
            assertEquals(rec2.value, avroReader.read().get("value").toString());

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void enumObject() throws IOException {

            record EnumObject(Category value) {
            }

            var rec1 = new EnumObject(Category.one);
            var rec2 = new EnumObject(Category.two);
            var writerTest = new ParquetWriterTest<>(EnumObject.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.value.name(), avroReader.read().get("value").toString());
            assertEquals(rec2.value.name(), avroReader.read().get("value").toString());

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void uuidObject() throws IOException {

            record UuidObject(UUID value) {
            }

            var rec1 = new UuidObject(UUID.randomUUID());
            var rec2 = new UuidObject(UUID.randomUUID());
            var writerTest = new ParquetWriterTest<>(UuidObject.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.value.toString(), avroReader.read().get("value").toString());
            assertEquals(rec2.value.toString(), avroReader.read().get("value").toString());

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void integerNullObject() throws IOException {

            record IntegerNullObject(Integer value) {
            }

            var rec1 = new IntegerNullObject(1);
            var rec2 = new IntegerNullObject(null);
            var writerTest = new ParquetWriterTest<>(IntegerNullObject.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.value, avroReader.read().get("value"));
            assertNull(avroReader.read().get("value"));

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void longNullObject() throws IOException {

            record LongNullObject(Long value) {
            }

            var rec1 = new LongNullObject(191919191919L);
            var rec2 = new LongNullObject(null);
            var writerTest = new ParquetWriterTest<>(LongNullObject.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.value, avroReader.read().get("value"));
            assertNull(avroReader.read().get("value"));

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void doubleNullObject() throws IOException {

            record DoubleNullObject(Double value) {
            }

            var rec1 = new DoubleNullObject(1.9);
            var rec2 = new DoubleNullObject(null);
            var writerTest = new ParquetWriterTest<>(DoubleNullObject.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.value, avroReader.read().get("value"));
            assertNull(avroReader.read().get("value"));

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void floatNullObject() throws IOException {

            record FloatNullObject(Float value) {
            }

            var rec1 = new FloatNullObject(1.9f);
            var rec2 = new FloatNullObject(null);
            var writerTest = new ParquetWriterTest<>(FloatNullObject.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.value, avroReader.read().get("value"));
            assertNull(avroReader.read().get("value"));

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void shortNullObject() throws IOException {

            record ShortNullObject(Short value) {
            }

            var rec1 = new ShortNullObject((short) 1);
            var rec2 = new ShortNullObject(null);
            var writerTest = new ParquetWriterTest<>(ShortNullObject.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.value, ((Number) avroReader.read().get("value")).shortValue());
            assertNull(avroReader.read().get("value"));

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void byteNullObject() throws IOException {

            record ByteNullObject(Byte value) {
            }

            var rec1 = new ByteNullObject((byte) 1);
            var rec2 = new ByteNullObject(null);
            var writerTest = new ParquetWriterTest<>(ByteNullObject.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.value, ((Number) avroReader.read().get("value")).byteValue());
            assertNull(avroReader.read().get("value"));

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void booleanNullObject() throws IOException {

            record BooleanNullObject(Boolean value) {
            }

            var rec1 = new BooleanNullObject(true);
            var rec2 = new BooleanNullObject(null);
            var writerTest = new ParquetWriterTest<>(BooleanNullObject.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.value, avroReader.read().get("value"));
            assertNull(avroReader.read().get("value"));

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void stringNullObject() throws IOException {

            record StringNullObject(String value) {
            }

            var rec1 = new StringNullObject("Madrid");
            var rec2 = new StringNullObject(null);
            var writerTest = new ParquetWriterTest<>(StringNullObject.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.value, avroReader.read().get("value").toString());
            assertNull(avroReader.read().get("value"));

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void enumNullObject() throws IOException {

            record EnumNullObject(Category value) {
            }

            var rec1 = new EnumNullObject(Category.one);
            var rec2 = new EnumNullObject(null);
            var writerTest = new ParquetWriterTest<>(EnumNullObject.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.value.name(), avroReader.read().get("value").toString());
            assertNull(avroReader.read().get("value"));

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void uuidNullObject() throws IOException {

            record UuidNullObject(UUID value) {
            }

            var rec1 = new UuidNullObject(UUID.randomUUID());
            var rec2 = new UuidNullObject(null);
            var writerTest = new ParquetWriterTest<>(UuidNullObject.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.value.toString(), avroReader.read().get("value").toString());
            assertEquals(null, avroReader.read().get("value"));

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }
    }

    @Nested
    class InListTypes {

        @Test
        void integerList() throws IOException {

            record IntegerList(List<Integer> values) {
            }

            var rec1 = new IntegerList(asList(1, null, 3));
            var rec2 = new IntegerList(null);
            var writerTest = new ParquetWriterTest<>(IntegerList.class);
            writerTest.write(rec1, rec2);

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void longList() throws IOException {

            record LongList(List<Long> values) {
            }

            var rec1 = new LongList(asList(191919191919L, null, 28282L));
            var rec2 = new LongList(null);
            var writerTest = new ParquetWriterTest<>(LongList.class);
            writerTest.write(rec1, rec2);

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void doubleList() throws IOException {

            record DoubleList(List<Double> values) {
            }

            var rec1 = new DoubleList(asList(1.9, null, 2.9));
            var rec2 = new DoubleList(null);
            var writerTest = new ParquetWriterTest<>(DoubleList.class);
            writerTest.write(rec1, rec2);

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void floatList() throws IOException {

            record FloatList(List<Float> values) {
            }

            var rec1 = new FloatList(asList(1.9f, null, 2.9f));
            var rec2 = new FloatList(null);
            var writerTest = new ParquetWriterTest<>(FloatList.class);
            writerTest.write(rec1, rec2);

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void shortList() throws IOException {

            record ShortList(List<Short> values) {
            }

            var rec1 = new ShortList(asList((short) 1, null, (short) 3));
            var rec2 = new ShortList(null);
            var writerTest = new ParquetWriterTest<>(ShortList.class);
            writerTest.write(rec1, rec2);

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void byteList() throws IOException {

            record ByteList(List<Byte> values) {
            }

            var rec1 = new ByteList(asList((byte) 1, null, (byte) 3));
            var rec2 = new ByteList(null);
            var writerTest = new ParquetWriterTest<>(ByteList.class);
            writerTest.write(rec1, rec2);

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void booleanList() throws IOException {

            record BooleanList(List<Boolean> values) {
            }

            var rec1 = new BooleanList(asList(true, null, false));
            var rec2 = new BooleanList(null);
            var writerTest = new ParquetWriterTest<>(BooleanList.class);
            writerTest.write(rec1, rec2);

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void stringList() throws IOException {

            record StringList(List<String> values) {
            }

            var rec1 = new StringList(asList("Madrid", null, "Zaragoza"));
            var rec2 = new StringList(null);
            var writerTest = new ParquetWriterTest<>(StringList.class);
            writerTest.write(rec1, rec2);

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void enumList() throws IOException {

            record EnumList(List<Category> values) {
            }

            var rec1 = new EnumList(asList(Category.one, null, Category.two));
            var rec2 = new EnumList(null);
            var writerTest = new ParquetWriterTest<>(EnumList.class);
            writerTest.write(rec1, rec2);

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void uuidList() throws IOException {

            record UuidList(List<UUID> values) {
            }

            var rec1 = new UuidList(asList(UUID.randomUUID(), null, UUID.randomUUID()));
            var rec2 = new UuidList(null);
            var writerTest = new ParquetWriterTest<>(UuidList.class);
            writerTest.write(rec1, rec2);

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

    }

    @Test
    void emptyFile() throws IOException {

        record EmptyFile(String someValue) {

        }
        var writerTest = new ParquetWriterTest<>(EmptyFile.class);
        writerTest.write();

        var avroReader = writerTest.getAvroGenericRecordReader();
        assertNull(avroReader.read());

        var carpetReader = writerTest.getCarpetReader();
        assertNull(carpetReader.read());
    }

    @Test
    void classWithMultipleFields() throws IOException {

        record ClassWithMultipleFields(String name, Category category,
                int p1, Integer f1, long p2, Long f2, double p3, Double f3, float p4, Float f4,
                byte p5, Byte f5, short p6, Short f6, boolean p7, Boolean f7) {
        }

        var rec = new ClassWithMultipleFields("Apple", Category.one,
                1, 2, 3L, 4L, 5.1, 6.2, 7.3f, 8.4f,
                (byte) 9, (byte) 10, (short) 11, (short) 12, true, false);

        var writerTest = new ParquetWriterTest<>(ClassWithMultipleFields.class);
        writerTest.write(rec);

        var avroReader = writerTest.getAvroGenericRecordReader();
        GenericRecord record = avroReader.read();
        assertEquals(rec.name, record.get("name").toString());
        assertEquals(rec.category.name(), record.get("category").toString());
        assertEquals(rec.p1, record.get("p1"));
        assertEquals(rec.f1, record.get("f1"));
        assertEquals(rec.p2, record.get("p2"));
        assertEquals(rec.f2, record.get("f2"));
        assertEquals(rec.p3, record.get("p3"));
        assertEquals(rec.f3, record.get("f3"));
        assertEquals(rec.p4, record.get("p4"));
        assertEquals(rec.f4, record.get("f4"));
        assertEquals(rec.p5, ((Number) record.get("p5")).byteValue());
        assertEquals(rec.f5, ((Number) record.get("f5")).byteValue());
        assertEquals(rec.p6, ((Number) record.get("p6")).shortValue());
        assertEquals(rec.f6, ((Number) record.get("f6")).shortValue());
        assertEquals(rec.p7, record.get("p7"));
        assertEquals(rec.f7, record.get("f7"));

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void classWithMultipleNullFields() throws IOException {

        record classWithMultipleNullFields(String name, Category category,
                int p1, Integer f1, long p2, Long f2, double p3, Double f3, float p4, Float f4,
                byte p5, Byte f5, short p6, Short f6, boolean p7, Boolean f7) {
        }

        var rec = new classWithMultipleNullFields(null, null,
                101, null, 103L, null, 105.1, null, 107.3f, null,
                (byte) 109, null, (short) 1011, null, false, null);
        var writerTest = new ParquetWriterTest<>(classWithMultipleNullFields.class);
        writerTest.write(rec);

        var avroReader = writerTest.getAvroGenericRecordReader();
        GenericRecord record = avroReader.read();
        assertNull(record.get("name"));
        assertNull(record.get("category"));
        assertEquals(rec.p1, record.get("p1"));
        assertNull(record.get("f1"));
        assertEquals(rec.p2, record.get("p2"));
        assertNull(record.get("f2"));
        assertEquals(rec.p3, record.get("p3"));
        assertNull(record.get("f3"));
        assertEquals(rec.p4, record.get("p4"));
        assertNull(record.get("f4"));
        assertEquals(rec.p5, ((Number) record.get("p5")).byteValue());
        assertNull(record.get("f5"));
        assertEquals(rec.p6, ((Number) record.get("p6")).shortValue());
        assertNull(record.get("f6"));
        assertEquals(rec.p7, record.get("p7"));
        assertNull(record.get("f7"));

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Nested
    class GenericFieldsAreNotSupported {

        @Test
        void classWithGenericField() throws IOException {

            record ClassWithGenericField<T>(String name, T value) {
            }

            var rec1 = new ClassWithGenericField<>("Apple", 1);
            var rec2 = new ClassWithGenericField<>("Google", 2);
            var writerTest = new ParquetWriterTest<>(ClassWithGenericField.class);
            assertThrows(RecordTypeConversionException.class, () -> writerTest.write(rec1, rec2));
        }

    }

    @Nested
    class CompositedClasses {

        @Test
        void compositeValue() throws IOException {

            record Child(String id, int value) {
            }
            record CompositeMain(String name, Child child) {
            }

            CompositeMain rec1 = new CompositeMain("Madrid", new Child("Population", 100));
            CompositeMain rec2 = new CompositeMain("Santander", new Child("Population", 200));

            var writerTest = new ParquetWriterTest<>(CompositeMain.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            GenericRecord record = avroReader.read();
            assertEquals(rec1.name, record.get("name").toString());
            GenericRecord child = (GenericRecord) record.get("child");
            assertEquals(rec1.child.id, child.get("id").toString());
            assertEquals(rec1.child.value, child.get("value"));
            record = avroReader.read();
            assertEquals(rec2.name, record.get("name").toString());
            child = (GenericRecord) record.get("child");
            assertEquals(rec2.child.id, child.get("id").toString());
            assertEquals(rec2.child.value, child.get("value"));

            var carpetReader = writerTest.getCarpetReader();
            assertEquals(rec1, carpetReader.read());
            assertEquals(rec2, carpetReader.read());
        }

        @Test
        void compositeNullValue() throws IOException {

            record Child(String id, int value) {
            }
            record CompositeNullMain(String name, Child child) {
            }

            CompositeNullMain rec1 = new CompositeNullMain("Madrid", new Child("Age", 100));
            CompositeNullMain rec2 = new CompositeNullMain("Santander", null);

            var writerTest = new ParquetWriterTest<>(CompositeNullMain.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            GenericRecord record = avroReader.read();
            assertEquals(rec1.name, record.get("name").toString());
            GenericRecord child = (GenericRecord) record.get("child");
            assertEquals(rec1.child.id, child.get("id").toString());
            assertEquals(rec1.child.value, child.get("value"));
            record = avroReader.read();
            assertEquals(rec2.name, record.get("name").toString());
            assertNull(record.get("child"));
        }

        @Test
        void compositeGenericNotSupported() throws IOException {

            interface Some {
                String id();
            }

            record Child(String id, int value) implements Some {
            }

            // Just for testing, Record types are final and can not be extended
            record CompositeGeneric<T extends Some>(String name, T child) {
            }

            CompositeGeneric<Some> rec1 = new CompositeGeneric<>("Madrid", new Child("Age", 100));
            CompositeGeneric<Some> rec2 = new CompositeGeneric<>("Santander", null);

            var writerTest = new ParquetWriterTest<>(CompositeGeneric.class);
            assertThrows(RecordTypeConversionException.class, () -> writerTest.write(rec1, rec2));
        }

        public record RecursiveLoop(String id, RercursiveMain recursive) {
        }

        public record RecursiveChild(String id, RecursiveLoop child) {
        }

        public record RercursiveMain(String name, RecursiveChild child) {
        }

        @Test
        void recursiveCompositeNotSupported() throws IOException {

            RercursiveMain tail = new RercursiveMain("Tail", null);
            RecursiveLoop child2 = new RecursiveLoop("Level 3", tail);
            RecursiveChild child = new RecursiveChild("Level 2", child2);
            RercursiveMain main = new RercursiveMain("Level 1", child);

            var writerTest = new ParquetWriterTest<>(RercursiveMain.class);
            assertThrows(RecordTypeConversionException.class, () -> writerTest.write(main, tail));
        }

        @Test
        void multipleLevelComposition() throws IOException {

            record ThirdLevel(String area, long value) {
            }
            record SecondLevel(String name, ThirdLevel subChild) {
            }
            record MultipleLevelComposition(String id, SecondLevel child) {
            }

            MultipleLevelComposition rec1 = new MultipleLevelComposition("First",
                    new SecondLevel("Second", new ThirdLevel("Third", 20102032L)));
            MultipleLevelComposition rec2 = new MultipleLevelComposition("One",
                    new SecondLevel("Two", null));
            MultipleLevelComposition rec3 = new MultipleLevelComposition("Uno", null);

            var writerTest = new ParquetWriterTest<>(MultipleLevelComposition.class);
            writerTest.write(rec1, rec2, rec3);

            var avroReader = writerTest.getAvroGenericRecordReader();
            GenericRecord record1 = avroReader.read();
            assertEquals(rec1.id, record1.get("id").toString());
            GenericRecord child1 = (GenericRecord) record1.get("child");
            assertEquals(rec1.child.name, child1.get("name").toString());
            GenericRecord subChild1 = (GenericRecord) child1.get("subChild");
            assertEquals(rec1.child.subChild.area, subChild1.get("area").toString());
            assertEquals(rec1.child.subChild.value, subChild1.get("value"));

            GenericRecord record2 = avroReader.read();
            assertEquals(rec2.id, record2.get("id").toString());
            GenericRecord child2 = (GenericRecord) record2.get("child");
            assertEquals(rec2.child.name, child2.get("name").toString());
            assertNull(child2.get("subChild"));

            GenericRecord record3 = avroReader.read();
            assertEquals(rec3.id, record3.get("id").toString());
            assertNull(record3.get("child"));
        }

    }

    @Nested
    class RecordVisibility {

        public record PublicDeclaredRecord(String id, int value) {
        }

        @Test
        void publicDeclaredRecord() throws IOException {
            var rec1 = new PublicDeclaredRecord("Madrid", 10);
            var rec2 = new PublicDeclaredRecord("Barcelona", 20);
            var writerTest = new ParquetWriterTest<>(PublicDeclaredRecord.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.id, avroReader.read().get("id").toString());
            assertEquals(rec2.id, avroReader.read().get("id").toString());
        }

        private record PrivateDeclaredRecord(String id, int value) {
        }

        @Test
        void privateDeclaredRecord() throws IOException {
            var rec1 = new PrivateDeclaredRecord("Madrid", 10);
            var rec2 = new PrivateDeclaredRecord("Barcelona", 20);
            var writerTest = new ParquetWriterTest<>(PrivateDeclaredRecord.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.id, avroReader.read().get("id").toString());
            assertEquals(rec2.id, avroReader.read().get("id").toString());
        }

        @Test
        void methodDeclaredRecord() throws IOException {
            record MethodDeclaredRecord(String id, int value) {
            }

            var rec1 = new MethodDeclaredRecord("Madrid", 10);
            var rec2 = new MethodDeclaredRecord("Barcelona", 20);
            var writerTest = new ParquetWriterTest<>(MethodDeclaredRecord.class);
            writerTest.write(rec1, rec2);

            var avroReader = writerTest.getAvroGenericRecordReader();
            assertEquals(rec1.id, avroReader.read().get("id").toString());
            assertEquals(rec2.id, avroReader.read().get("id").toString());
        }
    }

    @Nested
    class ColumnNameConversion {

        @Nested
        class Aliased {

            @Test
            void fieldAliased() throws IOException {
                record SomeClass(@Alias("foo") long someId, @Alias("bar") int operation, boolean active) {
                }

                var writerTest = new ParquetWriterTest<>(SomeClass.class);
                writerTest.write(new SomeClass(1L, 2, true));

                String expected = """
                        message SomeClass {
                          required int64 foo;
                          required int32 bar;
                          required boolean active;
                        }
                        """;
                assertEquals(expected, writerTest.getSchema().toString());

                record ReadClass(long foo, int bar, boolean active) {
                }
                ReadClass value = writerTest.getCarpetReader(ReadClass.class).read();
                assertEquals(new ReadClass(1L, 2, true), value);
            }
        }

        @Nested
        class ToSnakeCase {

            @Test
            void fromCamelCase() throws IOException {
                record SomeClass(long someId, int operationCode, int with3) {
                }

                var writerTest = new ParquetWriterTest<>(SomeClass.class)
                        .withNameStrategy(SNAKE_CASE);
                writerTest.write(new SomeClass(1L, 2, 3));

                String expected = """
                        message SomeClass {
                          required int64 some_id;
                          required int32 operation_code;
                          required int32 with3;
                        }
                        """;
                assertEquals(expected, writerTest.getSchema().toString());

                record Some_Class(long some_id, int operation_code, int with3) {
                }
                Some_Class value = writerTest.getCarpetReader(Some_Class.class).read();
                assertEquals(new Some_Class(1L, 2, 3), value);
            }

            @Test
            void readAgainWithSnakeCaseStrategy() throws IOException {
                record SomeClass(long someId, int operationCode, int with3) {
                }

                var writerTest = new ParquetWriterTest<>(SomeClass.class)
                        .withNameStrategy(SNAKE_CASE);
                writerTest.write(new SomeClass(1L, 2, 3));

                CarpetReader<SomeClass> reader = new CarpetReader<>(writerTest.getTestFile(), SomeClass.class)
                        .withFieldMatchingStrategy(FieldMatchingStrategy.SNAKE_CASE);
                SomeClass read = reader.stream().toList().get(0);
                assertEquals(new SomeClass(1L, 2, 3), read);
            }

            @Test
            void alreadySnakeCase() throws IOException {
                record SomeClass(long some_id, int operation_code, int with3) {
                }

                var writerTest = new ParquetWriterTest<>(SomeClass.class)
                        .withNameStrategy(SNAKE_CASE);
                writerTest.write(new SomeClass(1L, 2, 3));

                String expected = """
                        message SomeClass {
                          required int64 some_id;
                          required int32 operation_code;
                          required int32 with3;
                        }
                        """;
                assertEquals(expected, writerTest.getSchema().toString());

                assertEquals(new SomeClass(1L, 2, 3), writerTest.getCarpetReader().read());
            }

        }

    }

    @Test
    void writesMetadataInParquetFile() throws IOException {

        record SomeEntity(int value) {
        }

        var file = new File("/tmp/metadata.prquet");
        try (var writer = new CarpetWriter.Builder<>(new FileSystemOutputFile(file), SomeEntity.class)
                .withExtraMetaData("someKey", "someValue")
                .withExtraMetaData(Map.of("foo", "bar"))
                .build()) {
            writer.write(new SomeEntity(1));
        }

        ParquetFileReader reader = ParquetFileReader.open(new FileSystemInputFile(file));
        Map<String, String> metadata = reader.getFileMetaData().getKeyValueMetaData();
        assertEquals("someValue", metadata.get("someKey"));
        assertEquals("bar", metadata.get("foo"));
    }

}
