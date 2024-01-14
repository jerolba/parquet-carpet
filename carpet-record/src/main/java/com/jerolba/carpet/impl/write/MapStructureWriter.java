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

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.parquet.io.api.RecordConsumer;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.ParameterizedCollection;
import com.jerolba.carpet.impl.ParameterizedMap;

public class MapStructureWriter {

    private final RecordConsumer recordConsumer;
    private final CarpetWriteConfiguration carpetConfiguration;

    public MapStructureWriter(RecordConsumer recordConsumer, CarpetWriteConfiguration carpetConfiguration) {
        this.recordConsumer = recordConsumer;
        this.carpetConfiguration = carpetConfiguration;
    }

    public Consumer<Object> createMapWriter(ParameterizedMap parametized, RecordField recordField) {
        // Key
        BiConsumer<RecordConsumer, Object> elemKeyConsumer = null;
        Class<?> keyType = parametized.getKeyActualType();
        elemKeyConsumer = buildSimpleElementConsumer(keyType, recordConsumer, carpetConfiguration);

        // Value
        BiConsumer<RecordConsumer, Object> elemValueConsumer = null;
        if (parametized.valueIsCollection()) {
            ParameterizedCollection parametizedChild = parametized.getValueTypeAsCollection();
            Consumer<Object> childWriter = createCollectionWriter(parametizedChild, null);
            elemValueConsumer = (consumer, v) -> childWriter.accept(v);
        } else if (parametized.valueIsMap()) {
            ParameterizedMap parametizedChild = parametized.getValueTypeAsMap();
            var mapStructWriter = new MapStructureWriter(recordConsumer, carpetConfiguration);
            Consumer<Object> childWriter = mapStructWriter.createMapWriter(parametizedChild, null);
            elemValueConsumer = (consumer, v) -> childWriter.accept(v);
        } else {
            Class<?> valueType = parametized.getValueActualType();
            elemValueConsumer = buildSimpleElementConsumer(valueType, recordConsumer, carpetConfiguration);
        }
        if (elemValueConsumer == null || elemKeyConsumer == null) {
            throw new RecordTypeConversionException("Unsuported type in Map");
        }
        if (recordField != null) {
            return new MapRecordFieldWriter(recordField, elemKeyConsumer, elemValueConsumer);
        }
        // We are referenced by other collection
        var innerKeyStructureWriter = elemKeyConsumer;
        var innerValueStructureWriter = elemValueConsumer;
        return value -> {
            Map<?, ?> map = (Map<?, ?>) value;
            if (map != null) {
                recordConsumer.startGroup();
                if (!map.isEmpty()) {
                    writeKeyalueGroup(innerKeyStructureWriter, innerValueStructureWriter, map);
                }
                recordConsumer.endGroup();
            }
        };
    }

    private class MapRecordFieldWriter implements Consumer<Object> {

        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accesor;
        private final BiConsumer<RecordConsumer, Object> innerKeyStructureWriter;
        private final BiConsumer<RecordConsumer, Object> innerValueStructureWriter;

        MapRecordFieldWriter(RecordField recordField, BiConsumer<RecordConsumer, Object> innerStructureWriter,
                BiConsumer<RecordConsumer, Object> innerValueStructureWriter) {
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accesor = Reflection.recordAccessor(recordField.targetClass(), recordField.recordComponent());
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
                    writeKeyalueGroup(innerKeyStructureWriter, innerValueStructureWriter, value);
                }
                recordConsumer.endGroup();
                recordConsumer.endField(fieldName, idx);
            }
        }
    }

    private void writeKeyalueGroup(BiConsumer<RecordConsumer, Object> keyStructureWriter,
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

    private Consumer<Object> createCollectionWriter(ParameterizedCollection collectionClass, RecordField field) {
        return switch (carpetConfiguration.annotatedLevels()) {
        case ONE -> new OneLevelStructureWriter(recordConsumer, carpetConfiguration)
                .createCollectionWriter(collectionClass, field);
        case TWO -> new TwoLevelStructureWriter(recordConsumer, carpetConfiguration)
                .createCollectionWriter(collectionClass, field);
        case THREE -> new ThreeLevelStructureWriter(recordConsumer, carpetConfiguration)
                .createCollectionWriter(collectionClass, field);
        };
    }
}
