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

import static com.jerolba.carpet.impl.write.SimpleCollectionItemConsumerFactory.buildSimpleElementConsumer;

import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.parquet.io.api.RecordConsumer;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.ParameterizedCollection;
import com.jerolba.carpet.impl.ParameterizedMap;

class TwoLevelStructureWriter {

    private final RecordConsumer recordConsumer;
    private final CarpetWriteConfiguration carpetConfiguration;

    public TwoLevelStructureWriter(RecordConsumer recordConsumer, CarpetWriteConfiguration carpetConfiguration) {
        this.recordConsumer = recordConsumer;
        this.carpetConfiguration = carpetConfiguration;
    }

    public Consumer<Object> createCollectionWriter(ParameterizedCollection parametized, RecordField recordField) {
        BiConsumer<RecordConsumer, Object> elemConsumer = null;
        if (parametized.isCollection()) {
            ParameterizedCollection parametizedChild = parametized.getParametizedAsCollection();
            TwoLevelStructureWriter child = new TwoLevelStructureWriter(recordConsumer, carpetConfiguration);
            Consumer<Object> childWriter = child.createCollectionWriter(parametizedChild, null);
            elemConsumer = (consumer, v) -> childWriter.accept(v);
        } else if (parametized.isMap()) {
            ParameterizedMap parametizedChild = parametized.getParametizedAsMap();
            var mapStructWriter = new MapStructureWriter(recordConsumer, carpetConfiguration);
            Consumer<Object> childWriter = mapStructWriter.createMapWriter(parametizedChild, null);
            elemConsumer = (consumer, v) -> childWriter.accept(v);
        } else {
            Class<?> type = parametized.getActualType();
            elemConsumer = buildSimpleElementConsumer(type, recordConsumer, carpetConfiguration);
        }
        if (elemConsumer == null) {
            throw new RecordTypeConversionException("Unsuported type in collection");
        }
        if (recordField != null) {
            return new TwoLevelCollectionRecordFieldWriter(recordField, elemConsumer);
        }
        // We are referenced by other collection
        var innerStructureWriter = elemConsumer;
        return value -> {
            Collection<?> coll = (Collection<?>) value;
            if (coll != null && !coll.isEmpty()) {
                writeGroupElement(innerStructureWriter, coll);
            }
        };
    }

    private class TwoLevelCollectionRecordFieldWriter implements Consumer<Object> {

        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;
        private final BiConsumer<RecordConsumer, Object> innerStructureWriter;

        TwoLevelCollectionRecordFieldWriter(RecordField recordField,
                BiConsumer<RecordConsumer, Object> innerStructureWriter) {
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
                writeGroupElement(innerStructureWriter, coll);
                recordConsumer.endField(fieldName, idx);
            }
        }

    }

    private void writeGroupElement(BiConsumer<RecordConsumer, Object> innerStructureWriter, Collection<?> coll) {
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
