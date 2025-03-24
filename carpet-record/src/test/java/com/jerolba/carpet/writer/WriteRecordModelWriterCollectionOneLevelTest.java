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

import static com.jerolba.carpet.AnnotatedLevels.ONE;
import static com.jerolba.carpet.model.FieldTypes.BOOLEAN;
import static com.jerolba.carpet.model.FieldTypes.DOUBLE;
import static com.jerolba.carpet.model.FieldTypes.INTEGER;
import static com.jerolba.carpet.model.FieldTypes.LIST;
import static com.jerolba.carpet.model.FieldTypes.MAP;
import static com.jerolba.carpet.model.FieldTypes.SET;
import static com.jerolba.carpet.model.FieldTypes.STRING;
import static com.jerolba.carpet.model.FieldTypes.writeRecordModel;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.jerolba.carpet.ParquetWriterTest;
import com.jerolba.carpet.RecordTypeConversionException;

class WriteRecordModelWriterCollectionOneLevelTest {

    @Test
    void simpleTypeCollection() throws IOException {

        record SimpleTypeCollection(String name, List<Integer> ids, List<Double> amount) {
        }

        var mapper = writeRecordModel(SimpleTypeCollection.class)
                .withField("name", STRING, SimpleTypeCollection::name)
                .withField("ids", LIST.ofType(INTEGER), SimpleTypeCollection::ids)
                .withField("amount", LIST.ofType(DOUBLE), SimpleTypeCollection::amount);

        var rec = new SimpleTypeCollection("foo", List.of(1, 2, 3), List.of(1.2, 3.2));
        var writerTest = new ParquetWriterTest<>(SimpleTypeCollection.class).withLevel(ONE);
        writerTest.write(mapper, rec);

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
    }

    @Test
    void emptyCollectionIsTransformedToNull() throws IOException {

        record EmptyCollection(String name, List<Integer> ids) {
        }

        var mapper = writeRecordModel(EmptyCollection.class)
                .withField("name", STRING, EmptyCollection::name)
                .withField("ids", LIST.ofType(INTEGER), EmptyCollection::ids);

        var rec = new EmptyCollection("foo", List.of());
        var writerTest = new ParquetWriterTest<>(EmptyCollection.class).withLevel(ONE);
        writerTest.write(mapper, rec);

        var carpetReader = writerTest.getCarpetReader();
        EmptyCollection expectedNullList = new EmptyCollection("foo", null);
        assertEquals(expectedNullList, carpetReader.read());
    }

    @Test
    void consecutiveNestedCollectionsAreNotSupported() {

        record ConsecutiveNestedCollection(String id, List<List<Integer>> values) {
        }

        var mapper = writeRecordModel(ConsecutiveNestedCollection.class)
                .withField("id", STRING, ConsecutiveNestedCollection::id)
                .withField("values", LIST.ofType(LIST.ofType(INTEGER)), ConsecutiveNestedCollection::values);

        var rec = new ConsecutiveNestedCollection("foo", List.of(List.of(1, 2, 3)));
        assertThrows(RecordTypeConversionException.class, () -> {
            var writerTest = new ParquetWriterTest<>(ConsecutiveNestedCollection.class).withLevel(ONE);
            writerTest.write(mapper, rec);
        });
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
        var writerTest = new ParquetWriterTest<>(SimpleCompositeCollection.class).withLevel(ONE);
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
        var writerTest = new ParquetWriterTest<>(NonConsecutiveNestedCollection.class).withLevel(ONE);
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
        var writerTest = new ParquetWriterTest<>(MapInCollection.class).withLevel(ONE);
        writerTest.write(mapper, rec);

        var carpetReader = writerTest.getCarpetReader();
        assertEquals(rec, carpetReader.read());
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
        var writerTest = new ParquetWriterTest<>(SetCollection.class).withLevel(ONE);
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
        var writerTest = new ParquetWriterTest<>(SetCollection.class).withLevel(ONE);
        writerTest.write(mapper, rec);

        record ListCollection(String name, List<String> ids) {
        }

        var carpetReader = writerTest.getCarpetReader(ListCollection.class);
        assertEquals(new ListCollection("foo", ids), carpetReader.read());
    }

}
