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

import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.parquet.io.api.RecordConsumer;

import com.jerolba.carpet.RecordTypeConversionException;

class CollectionsWriters {

    private static final String LIST = "list";
    private static final String ELEMENT = "element";
    private static final String KEY = "key";
    private static final String VALUE = "value";
    private static final String KEY_VALUE = "key_value";

    private CollectionsWriters() {
    }

    abstract static class CollectionFieldWriter implements Consumer<Object> {

        protected final RecordConsumer recordConsumer;
        protected final String parquetFieldName;
        protected final int idx;
        protected final Function<Object, Object> accesor;
        protected final BiConsumer<RecordConsumer, Object> innerStructureWriter;

        CollectionFieldWriter(RecordConsumer recordConsumer, RecordField recordField,
                BiConsumer<RecordConsumer, Object> innerStructureWriter) {
            this.recordConsumer = recordConsumer;
            this.parquetFieldName = recordField.parquetFieldName();
            this.idx = recordField.idx();
            this.accesor = recordField.getAccessor();
            this.innerStructureWriter = innerStructureWriter;
        }

    }

    static class OneLevelCollectionFieldWriter extends CollectionFieldWriter {

        OneLevelCollectionFieldWriter(RecordConsumer recordConsumer, RecordField recordField,
                BiConsumer<RecordConsumer, Object> innerStructureWriter) {
            super(recordConsumer, recordField, innerStructureWriter);
        }

        @Override
        public void accept(Object object) {
            Collection<?> coll = (Collection<?>) accesor.apply(object);
            if (coll != null && !coll.isEmpty()) {
                recordConsumer.startField(parquetFieldName, idx);
                for (var v : coll) {
                    innerStructureWriter.accept(recordConsumer, v);
                }
                recordConsumer.endField(parquetFieldName, idx);
            }
        }
    }

    static Consumer<Object> twoLevelCollectionRecordFieldWriterFactory(
            RecordConsumer recordConsumer,
            RecordField recordField,
            BiConsumer<RecordConsumer, Object> elemConsumer) {
        if (recordField != null) {
            return new TwoLevelCollectionRecordFieldWriter(recordConsumer, recordField, elemConsumer);
        }
        // We are referenced by other collection
        return value -> {
            Collection<?> coll = (Collection<?>) value;
            if (coll != null) {
                writeGroupElementTwo(recordConsumer, elemConsumer, coll);
            }
        };
    }

    private static void writeGroupElementTwo(RecordConsumer recordConsumer,
            BiConsumer<RecordConsumer, Object> innerStructureWriter, Collection<?> collection) {

        recordConsumer.startGroup();
        if (!collection.isEmpty()) {
            recordConsumer.startField(ELEMENT, 0);
            for (var v : collection) {
                if (v == null) {
                    throw new NullPointerException("2-level list structures doesn't support null values");
                }
                innerStructureWriter.accept(recordConsumer, v);
            }
            recordConsumer.endField(ELEMENT, 0);
        }
        recordConsumer.endGroup();
    }

    private static class TwoLevelCollectionRecordFieldWriter extends CollectionFieldWriter {

        TwoLevelCollectionRecordFieldWriter(RecordConsumer recordConsumer, RecordField recordField,
                BiConsumer<RecordConsumer, Object> innerStructureWriter) {
            super(recordConsumer, recordField, innerStructureWriter);
        }

        @Override
        public void accept(Object object) {
            Collection<?> coll = (Collection<?>) accesor.apply(object);
            if (coll != null) {
                recordConsumer.startField(parquetFieldName, idx);
                writeGroupElementTwo(recordConsumer, innerStructureWriter, coll);
                recordConsumer.endField(parquetFieldName, idx);
            }
        }

    }

    static Consumer<Object> threeLevelCollectionRecordFieldWriterFactory(
            RecordConsumer recordConsumer,
            RecordField recordField,
            BiConsumer<RecordConsumer, Object> elemConsumer) {
        if (recordField != null) {
            return new ThreeLevelCollectionRecordFieldWriter(recordConsumer, recordField, elemConsumer);
        }
        // We are referenced by other collection
        return value -> {
            Collection<?> coll = (Collection<?>) value;
            if (coll != null) {
                recordConsumer.startGroup();
                if (!coll.isEmpty()) {
                    writeGroupElementThree(recordConsumer, elemConsumer, coll);
                }
                recordConsumer.endGroup();
            }
        };
    }

    private static void writeGroupElementThree(RecordConsumer recordConsumer,
            BiConsumer<RecordConsumer, Object> innerStructureWriter, Collection<?> collection) {

        recordConsumer.startField(LIST, 0);
        for (var v : collection) {
            recordConsumer.startGroup();
            if (v != null) {
                recordConsumer.startField(ELEMENT, 0);
                innerStructureWriter.accept(recordConsumer, v);
                recordConsumer.endField(ELEMENT, 0);
            }
            recordConsumer.endGroup();
        }
        recordConsumer.endField(LIST, 0);
    }

    private static class ThreeLevelCollectionRecordFieldWriter extends CollectionFieldWriter {

        ThreeLevelCollectionRecordFieldWriter(RecordConsumer recordConsumer, RecordField recordField,
                BiConsumer<RecordConsumer, Object> innerStructureWriter) {
            super(recordConsumer, recordField, innerStructureWriter);
        }

        @Override
        public void accept(Object object) {
            Collection<?> value = (Collection<?>) accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(parquetFieldName, idx);
                recordConsumer.startGroup();
                if (!value.isEmpty()) {
                    writeGroupElementThree(recordConsumer, innerStructureWriter, value);
                }
                recordConsumer.endGroup();
                recordConsumer.endField(parquetFieldName, idx);
            }
        }

    }

    static Consumer<Object> mapRecordFieldWriterFactory(
            RecordConsumer recordConsumer,
            RecordField recordField,
            BiConsumer<RecordConsumer, Object> elemKeyConsumer,
            BiConsumer<RecordConsumer, Object> elemValueConsumer) {

        if (elemValueConsumer == null || elemKeyConsumer == null) {
            throw new RecordTypeConversionException("Unsuported type in Map");
        }
        if (recordField != null) {
            return new MapRecordFieldWriter(recordConsumer, recordField, elemKeyConsumer, elemValueConsumer);
        }
        // We are referenced by other collection
        return value -> {
            if (value != null) {
                Map<?, ?> map = (Map<?, ?>) value;
                recordConsumer.startGroup();
                if (!map.isEmpty()) {
                    writeKeyValueGroup(recordConsumer, elemKeyConsumer, elemValueConsumer, map);
                }
                recordConsumer.endGroup();
            }
        };
    }

    private static void writeKeyValueGroup(RecordConsumer recordConsumer,
            BiConsumer<RecordConsumer, Object> keyStructureWriter,
            BiConsumer<RecordConsumer, Object> valueStructureWriter, Map<?, ?> map) {

        recordConsumer.startField(KEY_VALUE, 0);
        for (var v : map.entrySet()) {
            recordConsumer.startGroup();
            if (v.getKey() != null) {
                recordConsumer.startField(KEY, 0);
                keyStructureWriter.accept(recordConsumer, v.getKey());
                recordConsumer.endField(KEY, 0);
            }
            var mapValue = v.getValue();
            if (mapValue != null) {
                recordConsumer.startField(VALUE, 1);
                valueStructureWriter.accept(recordConsumer, mapValue);
                recordConsumer.endField(VALUE, 1);
            }
            recordConsumer.endGroup();
        }
        recordConsumer.endField(KEY_VALUE, 0);
    }

    static class MapRecordFieldWriter implements Consumer<Object> {

        private final RecordConsumer recordConsumer;
        private final String parquetFieldName;
        private final int idx;
        private final Function<Object, Object> accesor;
        private final BiConsumer<RecordConsumer, Object> innerKeyStructureWriter;
        private final BiConsumer<RecordConsumer, Object> innerValueStructureWriter;

        MapRecordFieldWriter(RecordConsumer recordConsumer, RecordField recordField,
                BiConsumer<RecordConsumer, Object> innerStructureWriter,
                BiConsumer<RecordConsumer, Object> innerValueStructureWriter) {
            this.recordConsumer = recordConsumer;
            this.parquetFieldName = recordField.parquetFieldName();
            this.idx = recordField.idx();
            this.accesor = recordField.getAccessor();
            this.innerKeyStructureWriter = innerStructureWriter;
            this.innerValueStructureWriter = innerValueStructureWriter;
        }

        @Override
        public void accept(Object object) {
            var value = (Map<?, ?>) accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(parquetFieldName, idx);
                recordConsumer.startGroup();
                if (!value.isEmpty()) {
                    writeKeyValueGroup(recordConsumer, innerKeyStructureWriter, innerValueStructureWriter, value);
                }
                recordConsumer.endGroup();
                recordConsumer.endField(parquetFieldName, idx);
            }
        }

    }

}
