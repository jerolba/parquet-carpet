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

import static com.jerolba.carpet.model.FieldTypes.BOOLEAN;
import static com.jerolba.carpet.model.FieldTypes.DOUBLE;
import static com.jerolba.carpet.model.FieldTypes.INTEGER;
import static com.jerolba.carpet.model.FieldTypes.LIST;
import static com.jerolba.carpet.model.FieldTypes.MAP;
import static com.jerolba.carpet.model.FieldTypes.STRING;
import static com.jerolba.carpet.model.FieldTypes.writeRecordModel;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.parquet.io.InvalidRecordException;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;

import com.jerolba.carpet.ParquetWriterTest;
import com.jerolba.carpet.annotation.ParquetEnum;
import com.jerolba.carpet.annotation.ParquetJson;

class WriteRecordModelWriterMapTest {

    enum Category {
        FOO, BAR
    }

    @Test
    void mapPrimitiveValue() throws IOException {

        record MapPrimitiveValue(String name, Map<String, Integer> ids, Map<String, Double> amount) {
        }

        var mapper = writeRecordModel(MapPrimitiveValue.class)
                .withField("name", STRING, MapPrimitiveValue::name)
                .withField("ids", MAP.ofTypes(STRING, INTEGER), MapPrimitiveValue::ids)
                .withField("amount", MAP.ofTypes(STRING, DOUBLE), MapPrimitiveValue::amount);

        var rec1 = new MapPrimitiveValue("foo", mapOf("ABCD", 1, "EFGH", 2), mapOf("ABCD", 1.2, "EFGH", 2.3));
        var rec2 = new MapPrimitiveValue("bar", mapOf("ABCD", 3, "EFGH", 4), mapOf("ABCD", 2.2, "EFGH", 3.3));
        var writerTest = new ParquetWriterTest<>(MapPrimitiveValue.class);
        writerTest.write(mapper, rec1, rec2);

        var avroReader = writerTest.getAvroGenericRecordReader();
        GenericRecord avroRecord = avroReader.read();
        assertEquals(rec1.name(), avroRecord.get("name").toString());
        assertEquals(rec1.ids(), unUtf8Map(avroRecord.get("ids")));
        assertEquals(rec1.amount(), unUtf8Map(avroRecord.get("amount")));

        avroRecord = avroReader.read();
        assertEquals(rec2.name(), avroRecord.get("name").toString());
        assertEquals(rec2.ids(), unUtf8Map(avroRecord.get("ids")));
        assertEquals(rec2.amount(), unUtf8Map(avroRecord.get("amount")));

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec1, carpetReader.read());
        assertEquals(rec2, carpetReader.read());
    }

    @Test
    void mapPrimitiveValueNull() throws IOException {

        record MapPrimitiveValueNull(String name, Map<String, Integer> ids, Map<String, Double> amount) {
        }

        var mapper = writeRecordModel(MapPrimitiveValueNull.class)
                .withField("name", STRING, MapPrimitiveValueNull::name)
                .withField("ids", MAP.ofTypes(STRING, INTEGER), MapPrimitiveValueNull::ids)
                .withField("amount", MAP.ofTypes(STRING, DOUBLE), MapPrimitiveValueNull::amount);

        var rec1 = new MapPrimitiveValueNull("foo", mapOf("ABCD", 1, "EFGH", 2), mapOf("ABCD", 1.2, "EFGH", 2.3));
        var rec2 = new MapPrimitiveValueNull("bar", mapOf("ABCD", null, "EFGH", 4), mapOf("ABCD", 2.2, "EFGH", null));
        var writerTest = new ParquetWriterTest<>(MapPrimitiveValueNull.class);
        writerTest.write(mapper, rec1, rec2);

        var avroReader = writerTest.getAvroGenericRecordReader();
        GenericRecord avroRecord = avroReader.read();
        assertEquals(rec1.name(), avroRecord.get("name").toString());
        assertEquals(rec1.ids(), unUtf8Map(avroRecord.get("ids")));
        assertEquals(rec1.amount(), unUtf8Map(avroRecord.get("amount")));

        avroRecord = avroReader.read();
        assertEquals(rec2.name(), avroRecord.get("name").toString());
        assertEquals(rec2.ids(), unUtf8Map(avroRecord.get("ids")));
        assertEquals(rec2.amount(), unUtf8Map(avroRecord.get("amount")));

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec1, carpetReader.read());
        assertEquals(rec2, carpetReader.read());
    }

    @Test
    void mapRecordValue() throws IOException {

        record ChildRecord(String id, boolean active) {
        }
        record MapRecordValue(String name, Map<String, ChildRecord> ids) {
        }

        var child = writeRecordModel(ChildRecord.class)
                .withField("id", STRING, ChildRecord::id)
                .withField("active", BOOLEAN.notNull(), ChildRecord::active);

        var mapper = writeRecordModel(MapRecordValue.class)
                .withField("name", STRING, MapRecordValue::name)
                .withField("ids", MAP.ofTypes(STRING, child), MapRecordValue::ids);

        var rec1 = new MapRecordValue("AVE", mapOf(
                "MAD", new ChildRecord("Madrid", true),
                "ZGZ", new ChildRecord("Zaragoza", false)));
        var rec2 = new MapRecordValue("AVE", mapOf(
                "MAD", new ChildRecord("Madrid", false),
                "CAT", new ChildRecord("Barcelona", true)));
        var writerTest = new ParquetWriterTest<>(MapRecordValue.class);
        writerTest.write(mapper, rec1, rec2);

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec1, carpetReader.read());
        assertEquals(rec2, carpetReader.read());
    }

    @Test
    void mapStringKeyAndValue() throws IOException {

        record MapValues(String name, Map<String, String> values) {
        }

        var rec1 = new MapValues("foo", Map.of("ABCD", "ONE", "EFGH", "TWO"));
        var rec2 = new MapValues("bar", Map.of("ABCD", "THREE", "EFGH", "FOUR"));
        var writerTest = new ParquetWriterTest<>(MapValues.class);
        writerTest.write(rec1, rec2);

        var avroReader = writerTest.getAvroGenericRecordReader();
        GenericRecord avroRecord = avroReader.read();
        assertEquals(rec1.name(), avroRecord.get("name").toString());
        assertEquals(rec1.values(), unUtf8Map(avroRecord.get("values")));

        avroRecord = avroReader.read();
        assertEquals(rec2.name(), avroRecord.get("name").toString());
        assertEquals(rec2.values(), unUtf8Map(avroRecord.get("values")));

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec1, carpetReader.read());
        assertEquals(rec2, carpetReader.read());
    }

    @Test
    void mapStringAsEnumValue() throws IOException {

        record MapValues(String name, Map<String, @ParquetEnum String> values) {
        }

        var rec1 = new MapValues("foo", Map.of("FOO", "BAR", "BAR", "FOO"));
        var rec2 = new MapValues("bar", Map.of("FOO", "FOO", "BAR", "BAR"));
        var writerTest = new ParquetWriterTest<>(MapValues.class);
        writerTest.write(rec1, rec2);

        var avroReader = writerTest.getAvroGenericRecordReader();
        GenericRecord avroRecord = avroReader.read();
        assertEquals(rec1.name(), avroRecord.get("name").toString());
        assertEquals(rec1.values(), unUtf8Map(avroRecord.get("values")));

        avroRecord = avroReader.read();
        assertEquals(rec2.name(), avroRecord.get("name").toString());
        assertEquals(rec2.values(), unUtf8Map(avroRecord.get("values")));

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec1, carpetReader.read());
        assertEquals(rec2, carpetReader.read());

        record MapValuesAsEnum(String name, Map<String, Category> values) {
        }

        var carpetReaderAsEnum = writerTest.getCarpetReader(MapValuesAsEnum.class);
        assertEquals(new MapValuesAsEnum("foo", Map.of("FOO", Category.BAR, "BAR", Category.FOO)),
                carpetReaderAsEnum.read());
        assertEquals(new MapValuesAsEnum("bar", Map.of("FOO", Category.FOO, "BAR", Category.BAR)),
                carpetReaderAsEnum.read());
    }

    @Test
    void mapEnumValue() throws IOException {

        record MapValues(String name, Map<String, Category> values) {
        }

        var rec1 = new MapValues("foo", Map.of("FOO", Category.BAR, "BAR", Category.FOO));
        var rec2 = new MapValues("bar", Map.of("FOO", Category.FOO, "BAR", Category.BAR));
        var writerTest = new ParquetWriterTest<>(MapValues.class);
        writerTest.write(rec1, rec2);

        var avroReader = writerTest.getAvroGenericRecordReader();
        GenericRecord avroRecord = avroReader.read();
        assertEquals(rec1.name(), avroRecord.get("name").toString());
        assertEquals(Map.of("FOO", "BAR", "BAR", "FOO"), unUtf8Map(avroRecord.get("values")));

        avroRecord = avroReader.read();
        assertEquals(rec2.name(), avroRecord.get("name").toString());
        assertEquals(Map.of("FOO", "FOO", "BAR", "BAR"), unUtf8Map(avroRecord.get("values")));

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec1, carpetReader.read());
        assertEquals(rec2, carpetReader.read());
    }

    @Test
    void mapJsonValue() throws IOException {

        record MapValues(String name, Map<String, @ParquetJson String> values) {
        }

        var rec1 = new MapValues("foo", Map.of("FOO", "{}", "BAR", "{\"key1\": \"value1\"}"));
        var rec2 = new MapValues("bar", Map.of("FOO", "{\"key2\": \"value2\"}", "BAR", "{}"));
        var writerTest = new ParquetWriterTest<>(MapValues.class);
        writerTest.write(rec1, rec2);

        var avroReader = writerTest.getAvroGenericRecordReader();
        GenericRecord avroRecord = avroReader.read();
        assertEquals(rec1.name(), avroRecord.get("name").toString());
        assertEquals(rec1.values(), unByteBufferMap(avroRecord.get("values")));

        avroRecord = avroReader.read();
        assertEquals(rec2.name(), avroRecord.get("name").toString());
        assertEquals(rec2.values(), unByteBufferMap(avroRecord.get("values")));

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec1, carpetReader.read());
        assertEquals(rec2, carpetReader.read());
    }

    @Test
    void mapBinaryValue() throws IOException {

        record MapValues(String name, Map<String, Binary> values) {
        }

        var rec1 = new MapValues("foo", Map.of("FOO", Binary.fromString("BAR"), "BAR", Binary.fromString("FOO")));
        var rec2 = new MapValues("bar", Map.of("FOO", Binary.fromString("FOO"), "BAR", Binary.fromString("BAR")));
        var writerTest = new ParquetWriterTest<>(MapValues.class);
        writerTest.write(rec1, rec2);

        var avroReader = writerTest.getAvroGenericRecordReader();
        GenericRecord avroRecord = avroReader.read();
        assertEquals(rec1.name(), avroRecord.get("name").toString());
        assertEquals(Map.of("FOO", "BAR", "BAR", "FOO"), unByteBufferMap(avroRecord.get("values")));

        avroRecord = avroReader.read();
        assertEquals(rec2.name(), avroRecord.get("name").toString());
        assertEquals(Map.of("FOO", "FOO", "BAR", "BAR"), unByteBufferMap(avroRecord.get("values")));

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec1, carpetReader.read());
        assertEquals(rec2, carpetReader.read());
    }

    @Test
    void mapRecordValueNull() throws IOException {

        record ChildRecord(String id, boolean active) {
        }
        record MapRecordValueNull(String name, Map<String, ChildRecord> ids) {
        }

        var child = writeRecordModel(ChildRecord.class)
                .withField("id", STRING, ChildRecord::id)
                .withField("active", BOOLEAN.notNull(), ChildRecord::active);

        var mapper = writeRecordModel(MapRecordValueNull.class)
                .withField("name", STRING, MapRecordValueNull::name)
                .withField("ids", MAP.ofTypes(STRING, child), MapRecordValueNull::ids);

        var rec1 = new MapRecordValueNull("AVE", mapOf(
                "MAD", new ChildRecord("Madrid", true),
                "ZGZ", new ChildRecord("Zaragoza", false)));
        var rec2 = new MapRecordValueNull("AVE", mapOf(
                "MAD", new ChildRecord("Madrid", true),
                "BDJ", null));

        var writerTest = new ParquetWriterTest<>(MapRecordValueNull.class);
        writerTest.write(mapper, rec1, rec2);

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec1, carpetReader.read());
        assertEquals(rec2, carpetReader.read());
    }

    @Test
    void mapRecordKeyNull() {

        record ChildRecord(String id, boolean active) {
        }
        record MapRecordValueNull(String name, Map<String, ChildRecord> ids) {
        }

        var child = writeRecordModel(ChildRecord.class)
                .withField("id", STRING, ChildRecord::id)
                .withField("active", BOOLEAN.notNull(), ChildRecord::active);

        var mapper = writeRecordModel(MapRecordValueNull.class)
                .withField("name", STRING, MapRecordValueNull::name)
                .withField("ids", MAP.ofTypes(STRING, child), MapRecordValueNull::ids);

        var rec1 = new MapRecordValueNull("AVE", mapOf(
                "MAD", new ChildRecord("Madrid", true),
                null, new ChildRecord("Heaven", false)));

        var writerTest = new ParquetWriterTest<>(MapRecordValueNull.class);
        assertThrows(InvalidRecordException.class, () -> writerTest.write(mapper, rec1));
    }

    @Test
    void nestedMap_MapPrimitiveValue() throws IOException {

        record NestedMap_MapPrimitiveValue(String id, Map<String, Map<String, Integer>> values) {
        }

        var mapper = writeRecordModel(NestedMap_MapPrimitiveValue.class)
                .withField("id", STRING, NestedMap_MapPrimitiveValue::id)
                .withField("values", MAP.ofTypes(STRING, MAP.ofTypes(STRING, INTEGER)),
                        NestedMap_MapPrimitiveValue::values);

        var rec1 = new NestedMap_MapPrimitiveValue("Plane", mapOf(
                "ABCD", mapOf("EFGH", 10, "IJKL", 20),
                "WXYZ", mapOf("EFGH", 30, "IJKL", 50)));
        var rec2 = new NestedMap_MapPrimitiveValue("Boat", mapOf(
                "ABCD", mapOf("EFGH", 40, "IJKL", 50),
                "WXYZ", mapOf("EFGH", 70, "IJKL", 90)));
        var writerTest = new ParquetWriterTest<>(NestedMap_MapPrimitiveValue.class);
        writerTest.write(mapper, rec1, rec2);

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec1, carpetReader.read());
        assertEquals(rec2, carpetReader.read());
    }

    @Test
    void nestedMap_MapPrimitiveValueNull() throws IOException {

        record NestedMap_MapPrimitiveValueNull(String id, Map<String, Map<String, Integer>> values) {
        }

        var mapper = writeRecordModel(NestedMap_MapPrimitiveValueNull.class)
                .withField("id", STRING, NestedMap_MapPrimitiveValueNull::id)
                .withField("values", MAP.ofTypes(STRING, MAP.ofTypes(STRING, INTEGER)),
                        NestedMap_MapPrimitiveValueNull::values);

        var rec1 = new NestedMap_MapPrimitiveValueNull("Plane", mapOf(
                "ABCD", mapOf("EFGH", 10, "IJKL", 20),
                "WXYZ", mapOf("EFGH", 30, "IJKL", 50)));
        var rec2 = new NestedMap_MapPrimitiveValueNull("Boat", mapOf(
                "ABCD", mapOf("EFGH", 40, "IJKL", null),
                "WXYZ", null));
        var writerTest = new ParquetWriterTest<>(NestedMap_MapPrimitiveValueNull.class);
        writerTest.write(mapper, rec1, rec2);

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec1, carpetReader.read());
        assertEquals(rec2, carpetReader.read());
    }

    @Test
    void nestedMap_MapRecordValue() throws IOException {

        record ChildRecord(String id, boolean active) {
        }
        record NestedMap_MapRecordValue(String name, Map<String, Map<String, ChildRecord>> ids) {
        }

        var child = writeRecordModel(ChildRecord.class)
                .withField("id", STRING, ChildRecord::id)
                .withField("active", BOOLEAN.notNull(), ChildRecord::active);

        var mapper = writeRecordModel(NestedMap_MapRecordValue.class)
                .withField("name", STRING, NestedMap_MapRecordValue::name)
                .withField("ids", MAP.ofTypes(STRING, MAP.ofTypes(STRING, child)), NestedMap_MapRecordValue::ids);

        var rec1 = new NestedMap_MapRecordValue("Trip", mapOf(
                "AA", mapOf("FF", new ChildRecord("Madrid", true), "200", new ChildRecord("Sevilla", false)),
                "BB", mapOf("JJ", new ChildRecord("Bilbao", false), "300", new ChildRecord("Zaragoza", false))));
        var rec2 = new NestedMap_MapRecordValue("Hotel", mapOf(
                "ZZ", mapOf("100", new ChildRecord("Madrid", true), "200", new ChildRecord("Sevilla", false)),
                "YY", mapOf("200", new ChildRecord("Bilbao", false), "300", new ChildRecord("Zaragoza", false))));
        var writerTest = new ParquetWriterTest<>(NestedMap_MapRecordValue.class);
        writerTest.write(mapper, rec1, rec2);

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec1, carpetReader.read());
        assertEquals(rec2, carpetReader.read());
    }

    @Test
    void nestedMap_MapRecordValueNull() throws IOException {

        record ChildRecord(String id, boolean active) {
        }
        record NestedMap_MapRecordValueNull(String name, Map<String, Map<String, ChildRecord>> ids) {
        }

        var child = writeRecordModel(ChildRecord.class)
                .withField("id", STRING, ChildRecord::id)
                .withField("active", BOOLEAN.notNull(), ChildRecord::active);

        var mapper = writeRecordModel(NestedMap_MapRecordValueNull.class)
                .withField("name", STRING, NestedMap_MapRecordValueNull::name)
                .withField("ids", MAP.ofTypes(STRING, MAP.ofTypes(STRING, child)), NestedMap_MapRecordValueNull::ids);

        var rec = new NestedMap_MapRecordValueNull("Hotel", mapOf(
                "ZZ", mapOf("100", new ChildRecord("Madrid", true), "200", null),
                "YY", null));
        var writerTest = new ParquetWriterTest<>(NestedMap_MapRecordValueNull.class);
        writerTest.write(mapper, rec);

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void nestedMap_Record_Map() throws IOException {

        record ChildWithMap(String name, Map<String, Integer> alias) {
        }
        record NestedMap_Record_Map(String id, Map<String, ChildWithMap> values) {
        }

        var child = writeRecordModel(ChildWithMap.class)
                .withField("name", STRING, ChildWithMap::name)
                .withField("alias", MAP.ofTypes(STRING, INTEGER), ChildWithMap::alias);

        var mapper = writeRecordModel(NestedMap_Record_Map.class)
                .withField("id", STRING, NestedMap_Record_Map::id)
                .withField("values", MAP.ofTypes(STRING, child), NestedMap_Record_Map::values);

        var rec = new NestedMap_Record_Map("foo", mapOf(
                "OS", new ChildWithMap("Apple", mapOf("MacOs", 1000, "OS X", 2000)),
                "CP", new ChildWithMap("MS", mapOf("Windows 10", 33, "Windows 11", 54))));
        var writerTest = new ParquetWriterTest<>(NestedMap_Record_Map.class);
        writerTest.write(mapper, rec);

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void mapKeyRecord() throws IOException {

        record KeyRecord(String id, boolean active) {
        }
        record MapKeyRecord(String name, Map<KeyRecord, String> ids) {
        }

        var keyRecord = writeRecordModel(KeyRecord.class)
                .withField("id", STRING, KeyRecord::id)
                .withField("active", BOOLEAN.notNull(), KeyRecord::active);

        var mapper = writeRecordModel(MapKeyRecord.class)
                .withField("name", STRING, MapKeyRecord::name)
                .withField("ids", MAP.ofTypes(keyRecord, STRING), MapKeyRecord::ids);

        var rec = new MapKeyRecord("foo", mapOf(
                new KeyRecord("Madrid", true), "MAD",
                new KeyRecord("Barcelona", true), "BCN"));
        var writerTest = new ParquetWriterTest<>(MapKeyRecord.class);
        writerTest.write(mapper, rec);

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void mapKeyRecord_ValueRecord() throws IOException {

        record KeyRecord(String id, boolean active) {
        }
        record ValueRecord(String id, int age) {
        }
        record MapKeyAndValueRecord(String name, Map<KeyRecord, ValueRecord> ids) {
        }

        var keyRecord = writeRecordModel(KeyRecord.class)
                .withField("id", STRING, KeyRecord::id)
                .withField("active", BOOLEAN.notNull(), KeyRecord::active);
        var valueRecord = writeRecordModel(ValueRecord.class)
                .withField("id", STRING, ValueRecord::id)
                .withField("age", INTEGER.notNull(), ValueRecord::age);
        var mapper = writeRecordModel(MapKeyAndValueRecord.class)
                .withField("name", STRING, MapKeyAndValueRecord::name)
                .withField("ids", MAP.ofTypes(keyRecord, valueRecord), MapKeyAndValueRecord::ids);

        var rec = new MapKeyAndValueRecord("Time", mapOf(
                new KeyRecord("Madrid", true), new ValueRecord("MAD", 10),
                new KeyRecord("Barcelona", true), new ValueRecord("MAD", 10)));
        var writerTest = new ParquetWriterTest<>(MapKeyAndValueRecord.class);
        writerTest.write(mapper, rec);

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void emptyMapIsTransformedToEmptyMap() throws IOException {

        record EmptyMap(String name, Map<String, Integer> ids) {
        }

        var mapper = writeRecordModel(EmptyMap.class)
                .withField("name", STRING, EmptyMap::name)
                .withField("ids", MAP.ofTypes(STRING, INTEGER), EmptyMap::ids);

        var rec = new EmptyMap("foo", Map.of());
        var writerTest = new ParquetWriterTest<>(EmptyMap.class);
        writerTest.write(mapper, rec);

        var carpetReader = writerTest.getCarpetReader();
        EmptyMap expectedEmptyMap = new EmptyMap("foo", emptyMap());
        assertEquals(expectedEmptyMap, carpetReader.read());
    }

    @Test
    void emptyNestedMapIsSupported() throws IOException {

        record EmptyNestedMap(String name, Map<String, Map<String, Integer>> ids) {
        }

        var mapper = writeRecordModel(EmptyNestedMap.class)
                .withField("name", STRING, EmptyNestedMap::name)
                .withField("ids", MAP.ofTypes(STRING, MAP.ofTypes(STRING, INTEGER)), EmptyNestedMap::ids);

        var rec = new EmptyNestedMap("foo", Map.of("key", Map.of()));
        var writerTest = new ParquetWriterTest<>(EmptyNestedMap.class);
        writerTest.write(mapper, rec);

        var carpetReader = writerTest.getCarpetReader();
        EmptyNestedMap expectedEmptyMap = new EmptyNestedMap("foo", Map.of("key", emptyMap()));
        assertEquals(expectedEmptyMap, carpetReader.read());
    }

    @Test
    void nullNestedMapIsSupported() throws IOException {

        record EmptyNestedMap(String name, Map<String, Map<String, Integer>> ids) {
        }

        var mapper = writeRecordModel(EmptyNestedMap.class)
                .withField("name", STRING, EmptyNestedMap::name)
                .withField("ids", MAP.ofTypes(STRING, MAP.ofTypes(STRING, INTEGER)), EmptyNestedMap::ids);

        Map<String, Map<String, Integer>> ids = new HashMap<>();
        ids.put("key", null);
        var rec = new EmptyNestedMap("foo", ids);
        var writerTest = new ParquetWriterTest<>(EmptyNestedMap.class);
        writerTest.write(mapper, rec);

        var carpetReader = writerTest.getCarpetReader();
        EmptyNestedMap expectedEmptyMap = new EmptyNestedMap("foo", ids);
        assertEquals(expectedEmptyMap, carpetReader.read());
    }

    @Test
    void emptyNestedListIsSupported() throws IOException {

        record EmptyNestedCollection(String name, Map<String, List<String>> ids) {
        }

        var mapper = writeRecordModel(EmptyNestedCollection.class)
                .withField("name", STRING, EmptyNestedCollection::name)
                .withField("ids", MAP.ofTypes(STRING, LIST.ofType(STRING)), EmptyNestedCollection::ids);

        var rec = new EmptyNestedCollection("foo", Map.of("key", List.of()));
        var writerTest = new ParquetWriterTest<>(EmptyNestedCollection.class);
        writerTest.write(mapper, rec);

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void nullNestedListIsSupported() throws IOException {

        record EmptyNestedCollection(String name, Map<String, List<String>> ids) {
        }

        var mapper = writeRecordModel(EmptyNestedCollection.class)
                .withField("name", STRING, EmptyNestedCollection::name)
                .withField("ids", MAP.ofTypes(STRING, LIST.ofType(STRING)), EmptyNestedCollection::ids);

        Map<String, List<String>> nullValue = new HashMap<>();
        nullValue.put("key", null);
        var rec = new EmptyNestedCollection("foo", nullValue);
        var writerTest = new ParquetWriterTest<>(EmptyNestedCollection.class);
        writerTest.write(mapper, rec);

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    // Map.of doesn't support null values
    private <T, R> Map<T, R> mapOf(T key1, R value1, T key2, R value2) {
        HashMap<T, R> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        return map;
    }

    private Object unByteBufferMap(Object obj) throws IOException {
        if (obj instanceof Map<?, ?> map) {
            Map<Object, Object> res = new HashMap<>();
            for (var e : map.entrySet()) {
                Object key = e.getKey() instanceof Utf8 u ? u.toString() : e.getKey();
                Object value = e.getValue() instanceof ByteBuffer u ? new String(u.array(), "UTF-8") : e.getValue();
                if (value instanceof Map) {
                    value = unByteBufferMap(value);
                }
                res.put(key, value);
            }
            return res;
        }
        return obj;
    }

    private Object unUtf8Map(Object obj) {
        if (obj instanceof Map<?, ?> map) {
            Map<Object, Object> res = new HashMap<>();
            for (var e : map.entrySet()) {
                Object key = e.getKey() instanceof Utf8 u ? u.toString() : e.getKey();
                Object value = e.getValue() instanceof Utf8 u ? u.toString() : e.getValue();
                if (value instanceof Map) {
                    value = unUtf8Map(value);
                }
                res.put(key, value);
            }
            return res;
        }
        return obj;
    }
}
