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

import static com.jerolba.carpet.ReadFlag.STRICT_NUMERIC_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.jerolba.carpet.ParquetWriterTest;
import com.jerolba.carpet.RecordTypeConversionException;

class CarpetReaderConversionTest {

    @Test
    void toDouble() throws IOException {

        record ToDouble(String name, double fromDouble, Double fromDoubleObj, float fromFloat, Float fromFloatObj) {
        }
        var rec1 = new ToDouble("Apple", 1.0, 2.0, 3.0f, 4.0f);
        var rec2 = new ToDouble("Sony", 1.0, null, 3.0f, null);
        var writerTest = new ParquetWriterTest<>(ToDouble.class);
        writerTest.write(rec1, rec2);

        record ToDoubleRead(String name, double fromDouble, Double fromDoubleObj,
                double fromFloat, Double fromFloatObj) {
        }

        var recExpected1 = new ToDoubleRead("Apple", 1.0, 2.0, 3.0, 4.0);
        var recExpected2 = new ToDoubleRead("Sony", 1.0, null, 3.0, null);
        var reader = writerTest.getCarpetReader(ToDoubleRead.class);
        assertEquals(recExpected1, reader.read());
        assertEquals(recExpected2, reader.read());
    }

    @Test
    void fromNaturalToDouble() throws IOException {

        record ToDouble(String name, int fromInteger, Integer fromIntegerObj, long fromLong, Long fromLongObj) {
        }
        var rec1 = new ToDouble("Apple", 1, 2, 3L, 4L);
        var rec2 = new ToDouble("Sony", 5, null, 6L, null);
        var writerTest = new ParquetWriterTest<>(ToDouble.class);
        writerTest.write(rec1, rec2);

        record ToDoubleRead(String name, double fromInteger, Double fromIntegerObj,
                double fromLong, Double fromLongObj) {
        }

        var recExpected1 = new ToDoubleRead("Apple", 1.0, 2.0, 3.0, 4.0);
        var recExpected2 = new ToDoubleRead("Sony", 5.0, null, 6.0, null);
        var reader = writerTest.getCarpetReader(ToDoubleRead.class);
        assertEquals(recExpected1, reader.read());
        assertEquals(recExpected2, reader.read());
    }

    @Test
    void toFloat() throws IOException {
        record ToFloat(String name, double fromDouble, Double fromDoubleObj, float fromFloat, Float fromFloatObj) {
        }
        var rec1 = new ToFloat("Apple", 1.0, 2.0, 3.0f, 4.0f);
        var rec2 = new ToFloat("Sony", 1.0, null, 3.0f, null);
        var writerTest = new ParquetWriterTest<>(ToFloat.class);
        writerTest.write(rec1, rec2);

        record ToFloatRead(String name, float fromDouble, Float fromDoubleObj, float fromFloat,
                Float fromFloatObj) {
        }

        var recExpected1 = new ToFloatRead("Apple", 1.0f, 2.0f, 3.0f, 4.0f);
        var recExpected2 = new ToFloatRead("Sony", 1.0f, null, 3.0f, null);
        var reader = writerTest.getCarpetReader(ToFloatRead.class);
        assertEquals(recExpected1, reader.read());
        assertEquals(recExpected2, reader.read());

        var readerStrict = writerTest.getCarpetReader(ToFloatRead.class, STRICT_NUMERIC_TYPE);
        assertThrows(RecordTypeConversionException.class, () -> readerStrict.read());
    }

    @Test
    void toLong() throws IOException {
        record ToLong(String name, long fromLong, Long fromLongObj, int fromInteger, Integer fromIntegerObj) {
        }
        var rec1 = new ToLong("Apple", 1L, 2L, 3, 4);
        var rec2 = new ToLong("Sony", 5, null, 6, null);
        var writerTest = new ParquetWriterTest<>(ToLong.class);
        writerTest.write(rec1, rec2);

        record ToLongRead(String name, long fromLong, Long fromLongObj, long fromInteger, Long fromIntegerObj) {
        }

        var recExpected1 = new ToLongRead("Apple", 1L, 2L, 3L, 4L);
        var recExpected2 = new ToLongRead("Sony", 5L, null, 6L, null);
        var reader = writerTest.getCarpetReader(ToLongRead.class);
        assertEquals(recExpected1, reader.read());
        assertEquals(recExpected2, reader.read());
    }

    @Test
    void toInteger() throws IOException {
        record ToInteger(String name, long fromLong, Long fromLongObj, int fromInteger, Integer fromIntegerObj) {
        }
        var rec1 = new ToInteger("Apple", 1L, 2L, 3, 4);
        var rec2 = new ToInteger("Sony", 5, null, 6, null);
        var writerTest = new ParquetWriterTest<>(ToInteger.class);
        writerTest.write(rec1, rec2);

        record ToIntegerRead(String name, int fromLong, Integer fromLongObj, int fromInteger,
                Integer fromIntegerObj) {
        }

        var recExpected1 = new ToIntegerRead("Apple", 1, 2, 3, 4);
        var recExpected2 = new ToIntegerRead("Sony", 5, null, 6, null);
        var reader = writerTest.getCarpetReader(ToIntegerRead.class);
        assertEquals(recExpected1, reader.read());
        assertEquals(recExpected2, reader.read());

        var readerStrict = writerTest.getCarpetReader(ToIntegerRead.class, STRICT_NUMERIC_TYPE);
        assertThrows(RecordTypeConversionException.class, () -> readerStrict.read());
    }

    @Test
    void toShort() throws IOException {
        record ToShort(String name, long fromLong, Long fromLongObj, int fromInteger, Integer fromIntegerObj) {
        }
        var rec1 = new ToShort("Apple", 1L, 2L, 3, 4);
        var rec2 = new ToShort("Sony", 5, null, 6, null);
        var writerTest = new ParquetWriterTest<>(ToShort.class);
        writerTest.write(rec1, rec2);

        record ToShortRead(String name, short fromLong, Short fromLongObj, short fromInteger,
                Short fromIntegerObj) {
        }

        var recExpected1 = new ToShortRead("Apple", (short) 1, (short) 2, (short) 3, (short) 4);
        var recExpected2 = new ToShortRead("Sony", (short) 5, null, (short) 6, null);
        var reader = writerTest.getCarpetReader(ToShortRead.class);
        assertEquals(recExpected1, reader.read());
        assertEquals(recExpected2, reader.read());

        var readerStrict = writerTest.getCarpetReader(ToShortRead.class, STRICT_NUMERIC_TYPE);
        assertThrows(RecordTypeConversionException.class, () -> readerStrict.read());
    }

    @Test
    void toByte() throws IOException {
        record ToByte(String name, long fromLong, Long fromLongObj, int fromInteger, Integer fromIntegerObj) {
        }
        var rec1 = new ToByte("Apple", 1L, 2L, 3, 4);
        var rec2 = new ToByte("Sony", 5, null, 6, null);
        var writerTest = new ParquetWriterTest<>(ToByte.class);
        writerTest.write(rec1, rec2);

        record ToByteRead(String name, byte fromLong, Byte fromLongObj, byte fromInteger, Byte fromIntegerObj) {
        }

        var recExpected1 = new ToByteRead("Apple", (byte) 1, (byte) 2, (byte) 3, (byte) 4);
        var recExpected2 = new ToByteRead("Sony", (byte) 5, null, (byte) 6, null);
        var reader = writerTest.getCarpetReader(ToByteRead.class);
        assertEquals(recExpected1, reader.read());
        assertEquals(recExpected2, reader.read());

        var readerStrict = writerTest.getCarpetReader(ToByteRead.class, STRICT_NUMERIC_TYPE);
        assertThrows(RecordTypeConversionException.class, () -> readerStrict.read());
    }

}
