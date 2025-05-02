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

import static com.jerolba.carpet.model.FieldTypes.BIG_DECIMAL;
import static com.jerolba.carpet.model.FieldTypes.BINARY;
import static com.jerolba.carpet.model.FieldTypes.BOOLEAN;
import static com.jerolba.carpet.model.FieldTypes.DOUBLE;
import static com.jerolba.carpet.model.FieldTypes.ENUM;
import static com.jerolba.carpet.model.FieldTypes.INTEGER;
import static com.jerolba.carpet.model.FieldTypes.LIST;
import static com.jerolba.carpet.model.FieldTypes.MAP;
import static com.jerolba.carpet.model.FieldTypes.SET;
import static com.jerolba.carpet.model.FieldTypes.STRING;
import static com.jerolba.carpet.model.FieldTypes.writeRecordModel;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;

import com.jerolba.carpet.ParquetWriterTest;
import com.jerolba.carpet.annotation.ParquetBson;
import com.jerolba.carpet.annotation.ParquetEnum;
import com.jerolba.carpet.annotation.ParquetJson;
import com.jerolba.carpet.writer.CarpetWriterCollectionThreeLevelTest.Category;

class WriteRecordModelWriterCollectionThreeLevelTest {

    @Test
    void simpleTypeCollection() throws IOException {

        record SimpleTypeCollection(String name, List<Integer> ids, List<Double> amount) {
        }

        var mapper = writeRecordModel(SimpleTypeCollection.class)
                .withField("name", STRING, SimpleTypeCollection::name)
                .withField("ids", LIST.ofType(INTEGER), SimpleTypeCollection::ids)
                .withField("amount", LIST.ofType(DOUBLE), SimpleTypeCollection::amount);

        var rec = new SimpleTypeCollection("foo", List.of(1, 2, 3), List.of(1.2, 3.2));
        var writerTest = new ParquetWriterTest<>(SimpleTypeCollection.class);
        writerTest.write(mapper, rec);

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void simpleStringCollection() throws IOException {

        record SimpleTypeCollection(String name, List<String> values) {
        }

        var mapper = writeRecordModel(SimpleTypeCollection.class)
                .withField("name", STRING, SimpleTypeCollection::name)
                .withField("values", LIST.ofType(STRING), SimpleTypeCollection::values);

        var rec = new SimpleTypeCollection("foo", List.of("foo", "bar"));
        var writerTest = new ParquetWriterTest<>(SimpleTypeCollection.class);
        writerTest.write(mapper, rec);

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void simpleStringAsEnumCollection() throws IOException {

        record SimpleTypeCollection(String name, List<@ParquetEnum String> values) {
        }

        var mapper = writeRecordModel(SimpleTypeCollection.class)
                .withField("name", STRING, SimpleTypeCollection::name)
                .withField("values", LIST.ofType(STRING.asEnum()), SimpleTypeCollection::values);

        var rec = new SimpleTypeCollection("foo", List.of("FOO", "BAR"));
        var writerTest = new ParquetWriterTest<>(SimpleTypeCollection.class);
        writerTest.write(mapper, rec);

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

        var mapper = writeRecordModel(SimpleTypeCollection.class)
                .withField("name", STRING, SimpleTypeCollection::name)
                .withField("values", LIST.ofType(ENUM.ofType(Category.class)), SimpleTypeCollection::values);

        var rec = new SimpleTypeCollection("foo", List.of(Category.FOO, Category.BAR));
        var writerTest = new ParquetWriterTest<>(SimpleTypeCollection.class);
        writerTest.write(mapper, rec);

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void simpleBigDecimalCollection() throws IOException {

        record SimpleTypeCollection(String name, List<BigDecimal> values) {
        }

        var mapper = writeRecordModel(SimpleTypeCollection.class)
                .withField("name", STRING, SimpleTypeCollection::name)
                .withField("values", LIST.ofType(BIG_DECIMAL), SimpleTypeCollection::values);

        var rec = new SimpleTypeCollection("foo", List.of(new BigDecimal("1.0"), new BigDecimal("2.0")));
        var writerTest = new ParquetWriterTest<>(SimpleTypeCollection.class)
                .withDecimalConfig(6, 2);
        writerTest.write(mapper, rec);

        var carpetReader = writerTest.getCarpetReader();
        var expected = new SimpleTypeCollection("foo", List.of(new BigDecimal("1.00"), new BigDecimal("2.00")));
        assertEquals(expected, carpetReader.read());
    }

    @Test
    void simpleBigDecimalAnnotatedCollection() throws IOException {

        record SimpleTypeCollection(String name, List<BigDecimal> values) {
        }

        var mapper = writeRecordModel(SimpleTypeCollection.class)
                .withField("name", STRING, SimpleTypeCollection::name)
                .withField("values", LIST.ofType(BIG_DECIMAL.withPrecisionScale(6, 3)), SimpleTypeCollection::values);

        var rec = new SimpleTypeCollection("foo", List.of(new BigDecimal("1.0"), new BigDecimal("2.0")));
        var writerTest = new ParquetWriterTest<>(SimpleTypeCollection.class);
        writerTest.write(mapper, rec);

        var carpetReader = writerTest.getCarpetReader();
        var expected = new SimpleTypeCollection("foo", List.of(new BigDecimal("1.000"), new BigDecimal("2.000")));
        assertEquals(expected, carpetReader.read());
    }

    @Test
    void simpleJsonCollection() throws IOException {

        record SimpleTypeCollection(String name, List<@ParquetJson String> values) {
        }

        var mapper = writeRecordModel(SimpleTypeCollection.class)
                .withField("name", STRING, SimpleTypeCollection::name)
                .withField("values", LIST.ofType(STRING.asJson()), SimpleTypeCollection::values);

        var rec = new SimpleTypeCollection("foo",
                List.of("{\"key\": 1, \"value\": \"foo\"}", "{\"key\": 2, \"value\": \"bar\"}"));
        var writerTest = new ParquetWriterTest<>(SimpleTypeCollection.class);
        writerTest.write(mapper, rec);

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void simpleBinaryCollection() throws IOException {

        record SimpleTypeCollection(String name, List<Binary> values) {
        }

        var mapper = writeRecordModel(SimpleTypeCollection.class)
                .withField("name", STRING, SimpleTypeCollection::name)
                .withField("values", LIST.ofType(BINARY), SimpleTypeCollection::values);

        byte[] binary1 = new byte[] { 1, 2 };
        byte[] binary2 = new byte[] { 3, 4 };
        var rec = new SimpleTypeCollection("foo",
                List.of(Binary.fromConstantByteArray(binary1), Binary.fromConstantByteArray(binary2)));
        var writerTest = new ParquetWriterTest<>(SimpleTypeCollection.class);
        writerTest.write(mapper, rec);

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void simpleBsonCollection() throws IOException {

        record SimpleTypeCollection(String name, List<@ParquetBson Binary> values) {
        }

        var mapper = writeRecordModel(SimpleTypeCollection.class)
                .withField("name", STRING, SimpleTypeCollection::name)
                .withField("values", LIST.ofType(BINARY.asBson()), SimpleTypeCollection::values);

        byte[] mockBson1 = new byte[] { 1, 2 };
        byte[] mockBson2 = new byte[] { 3, 4 };
        var rec = new SimpleTypeCollection("foo",
                List.of(Binary.fromConstantByteArray(mockBson1), Binary.fromConstantByteArray(mockBson2)));
        var writerTest = new ParquetWriterTest<>(SimpleTypeCollection.class);
        writerTest.write(mapper, rec);

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void consecutiveNestedCollections() throws IOException {

        record ConsecutiveNestedCollection(String id, List<List<Integer>> values) {
        }

        var mapper = writeRecordModel(ConsecutiveNestedCollection.class)
                .withField("id", STRING, ConsecutiveNestedCollection::id)
                .withField("values", LIST.ofType(LIST.ofType(INTEGER)), ConsecutiveNestedCollection::values);

        var rec = new ConsecutiveNestedCollection("foo", List.of(List.of(1, 2, 3)));
        var writerTest = new ParquetWriterTest<>(ConsecutiveNestedCollection.class);
        writerTest.write(mapper, rec);

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void simpleCompositeCollection() throws IOException {

        record ChildRecord(String id, boolean active) {
        }
        record SimpleCompositeCollection(String name, List<ChildRecord> ids) {
        }

        var child = writeRecordModel(ChildRecord.class)
                .withField("id", STRING, ChildRecord::id)
                .withField("active", BOOLEAN.notNull(), ChildRecord::active);

        var mapper = writeRecordModel(SimpleCompositeCollection.class)
                .withField("name", STRING, SimpleCompositeCollection::name)
                .withField("ids", LIST.ofType(child), SimpleCompositeCollection::ids);

        var rec = new SimpleCompositeCollection("foo",
                List.of(new ChildRecord("Madrid", true), new ChildRecord("Sevilla", false)));
        var writerTest = new ParquetWriterTest<>(SimpleCompositeCollection.class);
        writerTest.write(mapper, rec);

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void consecutiveNestedCompositeCollection() throws IOException {

        record ChildRecord(String id, boolean active) {
        }
        record ConsecutiveNestedCompositeCollection(String name, List<List<ChildRecord>> ids) {
        }

        var child = writeRecordModel(ChildRecord.class)
                .withField("id", STRING, ChildRecord::id)
                .withField("active", BOOLEAN.notNull(), ChildRecord::active);

        var mapper = writeRecordModel(ConsecutiveNestedCompositeCollection.class)
                .withField("name", STRING, ConsecutiveNestedCompositeCollection::name)
                .withField("ids", LIST.ofType(LIST.ofType(child)), ConsecutiveNestedCompositeCollection::ids);

        var rec = new ConsecutiveNestedCompositeCollection("foo",
                List.of(List.of(new ChildRecord("Madrid", true), new ChildRecord("Sevilla", false))));
        var writerTest = new ParquetWriterTest<>(ConsecutiveNestedCompositeCollection.class);
        writerTest.write(mapper, rec);

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void nonConsecutiveNestedCollections() throws IOException {

        record ChildCollection(String name, List<String> alias) {
        }
        record NonConsecutiveNestedCollection(String id, List<ChildCollection> values) {
        }

        var child = writeRecordModel(ChildCollection.class)
                .withField("name", STRING, ChildCollection::name)
                .withField("alias", LIST.ofType(STRING), ChildCollection::alias);

        var mapper = writeRecordModel(NonConsecutiveNestedCollection.class)
                .withField("id", STRING, NonConsecutiveNestedCollection::id)
                .withField("values", LIST.ofType(child), NonConsecutiveNestedCollection::values);

        var rec = new NonConsecutiveNestedCollection("foo",
                List.of(new ChildCollection("Apple", List.of("MacOs", "OS X"))));
        var writerTest = new ParquetWriterTest<>(NonConsecutiveNestedCollection.class);
        writerTest.write(mapper, rec);

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void nestedMapInCollection() throws IOException {

        record MapInCollection(String name, List<Map<String, Integer>> ids) {
        }

        var mapper = writeRecordModel(MapInCollection.class)
                .withField("name", STRING, MapInCollection::name)
                .withField("ids", LIST.ofType(MAP.ofTypes(STRING, INTEGER)), MapInCollection::ids);

        var rec = new MapInCollection("foo",
                List.of(Map.of("1", 1, "2", 2, "3", 3), Map.of("1", 10, "2", 20, "3", 30)));
        var writerTest = new ParquetWriterTest<>(MapInCollection.class);
        writerTest.write(mapper, rec);

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void emptyCollectionIsTransformedToEmptyCollection() throws IOException {

        record EmptyCollection(String name, List<Integer> ids) {
        }

        var mapper = writeRecordModel(EmptyCollection.class)
                .withField("name", STRING, EmptyCollection::name)
                .withField("ids", LIST.ofType(INTEGER), EmptyCollection::ids);

        var rec = new EmptyCollection("foo", List.of());
        var writerTest = new ParquetWriterTest<>(EmptyCollection.class);
        writerTest.write(mapper, rec);

        var carpetReader = writerTest.getCarpetReader();
        EmptyCollection expectedNullList = new EmptyCollection("foo", emptyList());
        assertEquals(expectedNullList, carpetReader.read());
    }

    @Test
    void emptyNestedCollectionIsSupported() throws IOException {

        record EmptyNestedCollection(String name, List<List<Integer>> ids) {
        }

        var mapper = writeRecordModel(EmptyNestedCollection.class)
                .withField("name", STRING, EmptyNestedCollection::name)
                .withField("ids", LIST.ofType(LIST.ofType(INTEGER)), EmptyNestedCollection::ids);

        var rec = new EmptyNestedCollection("foo", List.of(List.of()));
        var writerTest = new ParquetWriterTest<>(EmptyNestedCollection.class);
        writerTest.write(mapper, rec);

        var carpetReader = writerTest.getCarpetReader();
        EmptyNestedCollection expected = new EmptyNestedCollection("foo", List.of(List.of()));
        assertEquals(expected, carpetReader.read());
    }

    @Test
    void mixedCollection() throws IOException {

        record WithCollection(String name, List<Integer> ids) {
        }

        var mapper = writeRecordModel(WithCollection.class)
                .withField("name", STRING, WithCollection::name)
                .withField("ids", LIST.ofType(INTEGER), WithCollection::ids);

        var writerTest = new ParquetWriterTest<>(WithCollection.class);
        writerTest.write(mapper,
                new WithCollection("foo", null),
                new WithCollection("bar", List.of()),
                new WithCollection("baz", List.of(1, 2, 3)),
                new WithCollection("baz", asList(1, null, 3)));

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(new WithCollection("foo", null), carpetReader.read());
        assertEquals(new WithCollection("bar", List.of()), carpetReader.read());
        assertEquals(new WithCollection("baz", List.of(1, 2, 3)), carpetReader.read());
        assertEquals(new WithCollection("baz", asList(1, null, 3)), carpetReader.read());
    }

    @Test
    void setCollectionAsListIsSupported() throws IOException {

        record SetCollection(String name, Set<String> ids) {
        }

        var mapper = writeRecordModel(SetCollection.class)
                .withField("name", STRING, SetCollection::name)
                .withField("ids", LIST.ofType(STRING), SetCollection::ids);

        var ids = List.of("ONE", "TWO", "THREE");
        var rec = new SetCollection("foo", new HashSet<>(ids));
        var writerTest = new ParquetWriterTest<>(SetCollection.class);
        writerTest.write(mapper, rec);

        record ListCollection(String name, List<String> ids) {
        }

        var carpetReader = writerTest.getCarpetReader(ListCollection.class);
        assertEquals(new ListCollection("foo", ids), carpetReader.read());
    }

    @Test
    void setCollectionAsSetIsSupported() throws IOException {

        record SetCollection(String name, Set<String> ids) {
        }

        var mapper = writeRecordModel(SetCollection.class)
                .withField("name", STRING, SetCollection::name)
                .withField("ids", SET.ofType(STRING), SetCollection::ids);

        var ids = List.of("ONE", "TWO", "THREE");
        var rec = new SetCollection("foo", new HashSet<>(ids));
        var writerTest = new ParquetWriterTest<>(SetCollection.class);
        writerTest.write(mapper, rec);

        record ListCollection(String name, List<String> ids) {
        }

        var carpetReader = writerTest.getCarpetReader(ListCollection.class);
        assertEquals(new ListCollection("foo", ids), carpetReader.read());
    }

}
