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

import static com.jerolba.carpet.AnnotatedLevels.TWO;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;

import com.jerolba.carpet.ParquetWriterTest;
import com.jerolba.carpet.annotation.ParquetBson;
import com.jerolba.carpet.annotation.ParquetEnum;
import com.jerolba.carpet.annotation.ParquetJson;
import com.jerolba.carpet.writer.CarpetWriterCollectionThreeLevelTest.Category;

class CarpetWriterCollectionTwoLevelTest {

    @Test
    void simpleTypeCollection() throws IOException {

        record SimpleTypeCollection(String name, List<Integer> ids, List<Double> amount) {
        }

        var rec = new SimpleTypeCollection("foo", List.of(1, 2, 3), List.of(1.2, 3.2));
        var writerTest = new ParquetWriterTest<>(SimpleTypeCollection.class).withLevel(TWO);
        writerTest.write(rec);

        var avroReader = writerTest.getAvroGenericRecordReader();
        GenericRecord avroRecord = avroReader.read();
        assertEquals(rec.name(), avroRecord.get("name").toString());
        assertEquals(rec.ids(), avroRecord.get("ids"));
        assertEquals(rec.amount(), avroRecord.get("amount"));

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void simpleStringCollection() throws IOException {

        record SimpleTypeCollection(String name, List<String> values) {
        }

        var rec = new SimpleTypeCollection("foo", List.of("foo", "bar"));
        var writerTest = new ParquetWriterTest<>(SimpleTypeCollection.class).withLevel(TWO);
        writerTest.write(rec);

        var avroReader = writerTest.getAvroGenericRecordReader();
        GenericRecord avroRecord = avroReader.read();
        assertEquals(rec.name(), avroRecord.get("name").toString());

        Array<Utf8> ids = (Array<Utf8>) avroRecord.get("values");
        assertEquals(2, ids.size());
        assertEquals("foo", ids.get(0).toString());
        assertEquals("bar", ids.get(1).toString());
        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void simpleStringAsEnumCollection() throws IOException {

        record SimpleTypeCollection(String name, List<@ParquetEnum String> values) {
        }

        var rec = new SimpleTypeCollection("foo", List.of("FOO", "BAR"));
        var writerTest = new ParquetWriterTest<>(SimpleTypeCollection.class).withLevel(TWO);
        writerTest.write(rec);

        var avroReader = writerTest.getAvroGenericRecordReader();
        GenericRecord avroRecord = avroReader.read();
        assertEquals(rec.name(), avroRecord.get("name").toString());

        Array<Utf8> ids = (Array<Utf8>) avroRecord.get("values");
        assertEquals(2, ids.size());
        assertEquals("FOO", ids.get(0).toString());
        assertEquals("BAR", ids.get(1).toString());
        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());

        record AsEnum(String name, List<Category> values) {
        }

        var recEnum = new AsEnum("foo", List.of(Category.FOO, Category.BAR));
        var carpetReaderEnum = writerTest.getCarpetReader(AsEnum.class);
        assertEquals(recEnum, carpetReaderEnum.read());
    }

    @Test
    void simpleEnumCollection() throws IOException {

        record SimpleTypeCollection(String name, List<Category> values) {
        }

        var rec = new SimpleTypeCollection("foo", List.of(Category.FOO, Category.BAR));
        var writerTest = new ParquetWriterTest<>(SimpleTypeCollection.class).withLevel(TWO);
        writerTest.write(rec);

        var avroReader = writerTest.getAvroGenericRecordReader();
        GenericRecord avroRecord = avroReader.read();
        assertEquals(rec.name(), avroRecord.get("name").toString());

        Array<Utf8> ids = (Array<Utf8>) avroRecord.get("values");
        assertEquals(2, ids.size());
        assertEquals("FOO", ids.get(0).toString());
        assertEquals("BAR", ids.get(1).toString());
        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void simpleJsonCollection() throws IOException {

        record SimpleTypeCollection(String name, List<@ParquetJson String> values) {
        }

        var rec = new SimpleTypeCollection("foo",
                List.of("{\"key\": 1, \"value\": \"foo\"}", "{\"key\": 2, \"value\": \"bar\"}"));
        var writerTest = new ParquetWriterTest<>(SimpleTypeCollection.class).withLevel(TWO);
        writerTest.write(rec);

        var avroReader = writerTest.getAvroGenericRecordReader();
        GenericRecord avroRecord = avroReader.read();
        assertEquals(rec.name(), avroRecord.get("name").toString());

        Array<ByteBuffer> ids = (Array<ByteBuffer>) avroRecord.get("values");
        assertEquals(2, ids.size());
        // Avro does not support JSON, so we need to convert it to a string
        assertEquals(rec.values().get(0), new String(ids.get(0).array(), "UTF-8"));
        assertEquals(rec.values().get(1), new String(ids.get(1).array(), "UTF-8"));
        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void simpleBinaryCollection() throws IOException {

        record SimpleTypeCollection(String name, List<Binary> values) {
        }

        byte[] binary1 = new byte[] { 1, 2 };
        byte[] binary2 = new byte[] { 3, 4 };
        var rec = new SimpleTypeCollection("foo",
                List.of(Binary.fromConstantByteArray(binary1), Binary.fromConstantByteArray(binary2)));
        var writerTest = new ParquetWriterTest<>(SimpleTypeCollection.class).withLevel(TWO);
        writerTest.write(rec);

        var avroReader = writerTest.getAvroGenericRecordReader();
        GenericRecord avroRecord = avroReader.read();
        assertEquals(rec.name(), avroRecord.get("name").toString());

        Array<ByteBuffer> ids = (Array<ByteBuffer>) avroRecord.get("values");
        assertEquals(2, ids.size());
        byte[] fromAvro1 = ids.get(0).array();
        assertEquals(binary1.length, fromAvro1.length);
        for (int i = 0; i < binary1.length; i++) {
            assertEquals(binary1[i], fromAvro1[i]);
        }
        byte[] fromAvro2 = ids.get(1).array();
        assertEquals(binary2.length, fromAvro2.length);
        for (int i = 0; i < binary2.length; i++) {
            assertEquals(binary2[i], fromAvro2[i]);
        }
        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void simpleBsonCollection() throws IOException {

        record SimpleTypeCollection(String name, List<@ParquetBson Binary> values) {
        }

        byte[] mockBson1 = new byte[] { 1, 2 };
        byte[] mockBson2 = new byte[] { 3, 4 };
        var rec = new SimpleTypeCollection("foo",
                List.of(Binary.fromConstantByteArray(mockBson1), Binary.fromConstantByteArray(mockBson2)));
        var writerTest = new ParquetWriterTest<>(SimpleTypeCollection.class).withLevel(TWO);
        writerTest.write(rec);

        var avroReader = writerTest.getAvroGenericRecordReader();
        GenericRecord avroRecord = avroReader.read();
        assertEquals(rec.name(), avroRecord.get("name").toString());

        Array<ByteBuffer> ids = (Array<ByteBuffer>) avroRecord.get("values");
        assertEquals(2, ids.size());
        // Avro does not support BSON
        byte[] fromAvro1 = ids.get(0).array();
        assertEquals(mockBson1.length, fromAvro1.length);
        for (int i = 0; i < mockBson1.length; i++) {
            assertEquals(mockBson1[i], fromAvro1[i]);
        }
        byte[] fromAvro2 = ids.get(1).array();
        assertEquals(mockBson2.length, fromAvro2.length);
        for (int i = 0; i < mockBson2.length; i++) {
            assertEquals(mockBson2[i], fromAvro2[i]);
        }
        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void consecutiveNestedCollections() throws IOException {

        record ConsecutiveNestedCollection(String id, List<List<Integer>> values) {
        }

        var rec = new ConsecutiveNestedCollection("foo", List.of(List.of(1, 2, 3)));
        var writerTest = new ParquetWriterTest<>(ConsecutiveNestedCollection.class).withLevel(TWO);
        writerTest.write(rec);

        var avroReader = writerTest.getAvroGenericRecordReader();
        GenericRecord avroRecord = avroReader.read();
        assertEquals(rec.id(), avroRecord.get("id").toString());
        Array<Array<Integer>> mainArray = (Array<Array<Integer>>) avroRecord.get("values");
        assertEquals(1, mainArray.size());
        Array<Integer> nestedArray = mainArray.get(0);
        assertEquals(3, nestedArray.size());
        assertEquals(1, nestedArray.get(0));
        assertEquals(2, nestedArray.get(1));
        assertEquals(3, nestedArray.get(2));

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void simpleCompositeCollection() throws IOException {

        record ChildRecord(String id, boolean active) {
        }
        record SimpleCompositeCollection(String name, List<ChildRecord> ids) {
        }

        var rec = new SimpleCompositeCollection("foo",
                List.of(new ChildRecord("Madrid", true), new ChildRecord("Sevilla", false)));
        var writerTest = new ParquetWriterTest<>(SimpleCompositeCollection.class).withLevel(TWO);
        writerTest.write(rec);

        var avroReader = writerTest.getAvroGenericRecordReader();
        GenericRecord avroRecord = avroReader.read();
        assertEquals(rec.name(), avroRecord.get("name").toString());
        Array<GenericRecord> ids = (Array<GenericRecord>) avroRecord.get("ids");
        GenericRecord child = ids.get(0);
        assertEquals(rec.ids().get(0).id(), child.get("id").toString());
        assertEquals(rec.ids().get(0).active(), child.get("active"));
        child = ids.get(1);
        assertEquals(rec.ids().get(1).id(), child.get("id").toString());
        assertEquals(rec.ids().get(1).active(), child.get("active"));

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void consecutiveNestedCompositeCollection() throws IOException {

        record ChildRecord(String id, boolean active) {
        }
        record ConsecutiveNestedCompositeCollection(String name, List<List<ChildRecord>> ids) {
        }

        var rec = new ConsecutiveNestedCompositeCollection("foo",
                List.of(List.of(new ChildRecord("Madrid", true), new ChildRecord("Sevilla", false))));
        var writerTest = new ParquetWriterTest<>(ConsecutiveNestedCompositeCollection.class).withLevel(TWO);
        writerTest.write(rec);

        var avroReader = writerTest.getAvroGenericRecordReader();
        GenericRecord avroRecord = avroReader.read();
        assertEquals(rec.name(), avroRecord.get("name").toString());
        Array<Array<GenericRecord>> mainArray = (Array<Array<GenericRecord>>) avroRecord.get("ids");
        assertEquals(1, mainArray.size());
        Array<GenericRecord> nestedArray = mainArray.get(0);
        GenericRecord child = nestedArray.get(0);
        List<ChildRecord> childs = rec.ids().get(0);
        assertEquals(childs.get(0).id(), child.get("id").toString());
        assertEquals(childs.get(0).active(), child.get("active"));
        child = nestedArray.get(1);
        assertEquals(childs.get(1).id(), child.get("id").toString());
        assertEquals(childs.get(1).active(), child.get("active"));

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void nonConsecutiveNestedCollections() throws IOException {

        record ChildCollection(String name, List<String> alias) {
        }
        record NonConsecutiveNestedCollection(String id, List<ChildCollection> values) {
        }

        var rec = new NonConsecutiveNestedCollection("foo",
                List.of(new ChildCollection("Apple", List.of("MacOs", "OS X"))));
        var writerTest = new ParquetWriterTest<>(NonConsecutiveNestedCollection.class).withLevel(TWO);
        writerTest.write(rec);

        var avroReader = writerTest.getAvroGenericRecordReader();
        GenericRecord avroRecord = avroReader.read();
        assertEquals(rec.id(), avroRecord.get("id").toString());
        Array<GenericRecord> ids = (Array<GenericRecord>) avroRecord.get("values");
        GenericRecord child = ids.get(0);
        assertEquals(rec.values().get(0).name(), child.get("name").toString());
        Array<Utf8> alias = (Array<Utf8>) child.get("alias");
        assertEquals(rec.values().get(0).alias().get(0), alias.get(0).toString());
        assertEquals(rec.values().get(0).alias().get(1), alias.get(1).toString());

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void nestedMapInCollection() throws IOException {

        record MapInCollection(String name, List<Map<String, Integer>> ids) {
        }

        var rec = new MapInCollection("foo",
                List.of(Map.of("1", 1, "2", 2, "3", 3), Map.of("1", 10, "2", 20, "3", 30)));
        var writerTest = new ParquetWriterTest<>(MapInCollection.class).withLevel(TWO);
        writerTest.write(rec);

        var avroReader = writerTest.getAvroGenericRecordReader();
        GenericRecord avroRecord = avroReader.read();
        assertEquals(rec.name(), avroRecord.get("name").toString());
        List<Map<Utf8, Integer>> ids = (List<Map<Utf8, Integer>>) avroRecord.get("ids");
        Map<Utf8, Integer> map1 = ids.get(0);
        assertEquals(3, map1.size());
        assertEquals(1, map1.get(new Utf8("1")));
        assertEquals(2, map1.get(new Utf8("2")));
        assertEquals(3, map1.get(new Utf8("3")));

        Map<Utf8, Integer> map2 = ids.get(1);
        assertEquals(3, map2.size());
        assertEquals(10, map2.get(new Utf8("1")));
        assertEquals(20, map2.get(new Utf8("2")));
        assertEquals(30, map2.get(new Utf8("3")));

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void emptyCollectionIsTransformedToEmptyCollection() throws IOException {

        record EmptyCollection(String name, List<Integer> ids) {
        }

        var rec = new EmptyCollection("foo", List.of());
        var writerTest = new ParquetWriterTest<>(EmptyCollection.class).withLevel(TWO);
        writerTest.write(rec);

        var avroReader = writerTest.getAvroGenericRecordReader();
        GenericRecord avroRecord = avroReader.read();
        assertEquals(rec.name(), avroRecord.get("name").toString());
        assertEquals(emptyList(), avroRecord.get("ids"));

        var carpetReader = writerTest.getCarpetReader();
        EmptyCollection expectedNullList = new EmptyCollection("foo", emptyList());
        assertEquals(expectedNullList, carpetReader.read());
    }

    @Test
    void emptyNestedCollectionsAreSupported() throws IOException {

        record EmptyNestedCollection(String name, List<List<Integer>> ids) {
        }

        var rec = new EmptyNestedCollection("foo", List.of(List.of()));
        var writerTest = new ParquetWriterTest<>(EmptyNestedCollection.class).withLevel(TWO);
        writerTest.write(rec);

        var avroReader = writerTest.getAvroGenericRecordReader();
        GenericRecord avroRecord = avroReader.read();
        assertEquals(rec.name(), avroRecord.get("name").toString());
        assertEquals(List.of(List.of()), avroRecord.get("ids"));
    }

    @Test
    void mixedCollection() throws IOException {

        record WithCollection(String name, List<Integer> ids) {
        }

        var writerTest = new ParquetWriterTest<>(WithCollection.class).withLevel(TWO);
        writerTest.write(new WithCollection("foo", null),
                new WithCollection("bar", List.of()),
                new WithCollection("baz", List.of(1, 2, 3)));

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(new WithCollection("foo", null), carpetReader.read());
        assertEquals(new WithCollection("bar", List.of()), carpetReader.read());
        assertEquals(new WithCollection("baz", List.of(1, 2, 3)), carpetReader.read());
    }

    @Test
    void setCollectionIsSupported() throws IOException {

        record SetCollection(String name, Set<String> ids) {
        }

        var ids = List.of("ONE", "TWO", "THREE");
        var rec = new SetCollection("foo", new HashSet<>(ids));
        var writerTest = new ParquetWriterTest<>(SetCollection.class).withLevel(TWO);
        writerTest.write(rec);

        var avroReader = writerTest.getAvroGenericRecordReader();
        GenericRecord avroRecord = avroReader.read();
        assertEquals(rec.name(), avroRecord.get("name").toString());
        var asLst = ((GenericData.Array) avroRecord.get("ids"))
                .stream().map(Object::toString)
                .toList();
        assertEquals(ids, asLst);

        record ListCollection(String name, List<String> ids) {
        }

        var carpetReader = writerTest.getCarpetReader(ListCollection.class);
        assertEquals(new ListCollection("foo", ids), carpetReader.read());
    }
}
