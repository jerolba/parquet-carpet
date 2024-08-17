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
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.parquet.io.api.RecordConsumer;

class CollectionsWriters {

    static class OneLevelCollectionFieldWriter implements Consumer<Object> {

        private final RecordConsumer recordConsumer;
        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;
        private final BiConsumer<RecordConsumer, Object> consumer;

        OneLevelCollectionFieldWriter(RecordConsumer recordConsumer, RecordField recordField,
                BiConsumer<RecordConsumer, Object> consumer) {
            this.recordConsumer = recordConsumer;
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = recordField.getAccessor();
            this.consumer = consumer;
        }

        @Override
        public void accept(Object object) {
            Collection<?> coll = (Collection<?>) accesor.apply(object);
            if (coll != null && !coll.isEmpty()) {
                recordConsumer.startField(fieldName, idx);
                for (var v : coll) {
                    consumer.accept(recordConsumer, v);
                }
                recordConsumer.endField(fieldName, idx);
            }
        }
    }

    static class TwoLevelCollectionRecordFieldWriter implements Consumer<Object> {

        private final RecordConsumer recordConsumer;
        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;
        private final BiConsumer<RecordConsumer, Object> innerStructureWriter;

        TwoLevelCollectionRecordFieldWriter(RecordConsumer recordConsumer, RecordField recordField,
                BiConsumer<RecordConsumer, Object> innerStructureWriter) {
            this.recordConsumer = recordConsumer;
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = recordField.getAccessor();
            this.innerStructureWriter = innerStructureWriter;
        }

        @Override
        public void accept(Object object) {
            var value = accesor.apply(object);
            Collection<?> coll = (Collection<?>) value;
            if (coll != null && !coll.isEmpty()) {
                recordConsumer.startField(fieldName, idx);
                writeGroupElementTwo(recordConsumer, innerStructureWriter, coll);
                recordConsumer.endField(fieldName, idx);
            }
        }

        public static void writeGroupElementTwo(RecordConsumer recordConsumer,
                BiConsumer<RecordConsumer, Object> innerStructureWriter, Collection<?> coll) {

            recordConsumer.startGroup();
            recordConsumer.startField("element", 0);
            for (var v : coll) {
                if (v == null) {
                    throw new NullPointerException("2-level list structures doesn't support null values");
                }
                innerStructureWriter.accept(recordConsumer, v);
            }
            recordConsumer.endField("element", 0);
            recordConsumer.endGroup();
        }

    }

    static class ThreeLevelCollectionRecordFieldWriter implements Consumer<Object> {

        private final RecordConsumer recordConsumer;
        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;
        private final BiConsumer<RecordConsumer, Object> innerStructureWriter;

        ThreeLevelCollectionRecordFieldWriter(RecordConsumer recordConsumer, RecordField recordField,
                BiConsumer<RecordConsumer, Object> innerStructureWriter) {
            this.recordConsumer = recordConsumer;
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = recordField.getAccessor();
            this.innerStructureWriter = innerStructureWriter;
        }

        @Override
        public void accept(Object object) {
            Collection<?> value = (Collection<?>) accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(fieldName, idx);
                recordConsumer.startGroup();
                if (!value.isEmpty()) {
                    writeGroupElementThree(recordConsumer, innerStructureWriter, value);
                }
                recordConsumer.endGroup();
                recordConsumer.endField(fieldName, idx);
            }
        }

        public static void writeGroupElementThree(RecordConsumer recordConsumer,
                BiConsumer<RecordConsumer, Object> innerStructureWriter, Collection<?> coll) {

            recordConsumer.startField("list", 0);
            for (var v : coll) {
                recordConsumer.startGroup();
                if (v != null) {
                    recordConsumer.startField("element", 0);
                    innerStructureWriter.accept(recordConsumer, v);
                    recordConsumer.endField("element", 0);
                }
                recordConsumer.endGroup();
            }
            recordConsumer.endField("list", 0);
        }

    }

    static class MapRecordFieldWriter implements Consumer<Object> {
    
        private final RecordConsumer recordConsumer;
        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;
        private final BiConsumer<RecordConsumer, Object> innerKeyStructureWriter;
        private final BiConsumer<RecordConsumer, Object> innerValueStructureWriter;
    
        MapRecordFieldWriter(RecordConsumer recordConsumer, RecordField recordField,
                BiConsumer<RecordConsumer, Object> innerStructureWriter,
                BiConsumer<RecordConsumer, Object> innerValueStructureWriter) {
            this.recordConsumer = recordConsumer;
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = recordField.getAccessor();
            this.innerKeyStructureWriter = innerStructureWriter;
            this.innerValueStructureWriter = innerValueStructureWriter;
        }
    
        @Override
        public void accept(Object object) {
            var value = (Map<?, ?>) accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(fieldName, idx);
                recordConsumer.startGroup();
                if (!value.isEmpty()) {
                    writeKeyalueGroup(recordConsumer, innerKeyStructureWriter, innerValueStructureWriter, value);
                }
                recordConsumer.endGroup();
                recordConsumer.endField(fieldName, idx);
            }
        }
    
        static void writeKeyalueGroup(RecordConsumer recordConsumer,
                BiConsumer<RecordConsumer, Object> keyStructureWriter,
                BiConsumer<RecordConsumer, Object> valueStructureWriter, Map<?, ?> map) {
    
            recordConsumer.startField("key_value", 0);
            for (var v : map.entrySet()) {
                recordConsumer.startGroup();
                if (v.getKey() != null) {
                    recordConsumer.startField("key", 0);
                    keyStructureWriter.accept(recordConsumer, v.getKey());
                    recordConsumer.endField("key", 0);
                }
                var mapValue = v.getValue();
                if (mapValue != null) {
                    recordConsumer.startField("value", 1);
                    valueStructureWriter.accept(recordConsumer, mapValue);
                    recordConsumer.endField("value", 1);
                }
                recordConsumer.endGroup();
            }
            recordConsumer.endField("key_value", 0);
        }
    
    }

}
