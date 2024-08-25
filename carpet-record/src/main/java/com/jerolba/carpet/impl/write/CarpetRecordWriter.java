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

import static com.jerolba.carpet.impl.AliasField.getFieldName;
import static com.jerolba.carpet.impl.Parameterized.getParameterizedCollection;
import static com.jerolba.carpet.impl.Parameterized.getParameterizedMap;
import static com.jerolba.carpet.impl.write.CollectionsWriters.MapRecordFieldWriter.writeKeyalueGroup;
import static com.jerolba.carpet.impl.write.CollectionsWriters.ThreeLevelCollectionRecordFieldWriter.writeGroupElementThree;
import static com.jerolba.carpet.impl.write.CollectionsWriters.TwoLevelCollectionRecordFieldWriter.writeGroupElementTwo;
import static com.jerolba.carpet.impl.write.FieldsWriter.buildSimpleElementConsumer;

import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.parquet.io.api.RecordConsumer;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.ParameterizedCollection;
import com.jerolba.carpet.impl.ParameterizedMap;
import com.jerolba.carpet.impl.write.CollectionsWriters.OneLevelCollectionFieldWriter;
import com.jerolba.carpet.impl.write.CollectionsWriters.ThreeLevelCollectionRecordFieldWriter;
import com.jerolba.carpet.impl.write.CollectionsWriters.TwoLevelCollectionRecordFieldWriter;

public class CarpetRecordWriter {

    private final RecordConsumer recordConsumer;
    private final CarpetWriteConfiguration carpetConfiguration;

    private final List<Consumer<Object>> fieldWriters = new ArrayList<>();

    public CarpetRecordWriter(RecordConsumer recordConsumer, Class<?> recordClass,
            CarpetWriteConfiguration carpetConfiguration) {
        this.recordConsumer = recordConsumer;
        this.carpetConfiguration = carpetConfiguration;

        int idx = 0;
        for (RecordComponent attr : recordClass.getRecordComponents()) {
            RecordField f = new ReflectionRecordField(recordClass, attr, getFieldName(attr), idx);
            Class<?> type = attr.getType();
            Consumer<Object> writer = null;
            BiConsumer<RecordConsumer, Object> basicTypeWriter = buildSimpleElementConsumer(type, recordConsumer,
                    carpetConfiguration);
            if (basicTypeWriter != null) {
                writer = new FieldWriterConsumer(recordConsumer, f, basicTypeWriter);
            } else if (Collection.class.isAssignableFrom(type)) {
                ParameterizedCollection collectionClass = getParameterizedCollection(attr);
                writer = createCollectionWriter(collectionClass, f);
            } else if (Map.class.isAssignableFrom(type)) {
                ParameterizedMap mapClass = getParameterizedMap(attr);
                writer = createMapStructureWriter(mapClass, f);
            } else {
                throw new RuntimeException(type.getName() + " can not be serialized");
            }
            fieldWriters.add(writer);
            idx++;
        }
    }

    public void write(Object record) {
        for (var fieldWriter : fieldWriters) {
            fieldWriter.accept(record);
        }
    }

    private Consumer<Object> createCollectionWriter(ParameterizedCollection collectionClass, RecordField f) {
        return switch (carpetConfiguration.annotatedLevels()) {
        case ONE -> createOneLevelStructureWriter(collectionClass, f);
        case TWO -> createTwoLevelStructureWriter(collectionClass, f);
        case THREE -> createThreeLevelStructureWriter(collectionClass, f);
        };
    }

    private Consumer<Object> createOneLevelStructureWriter(ParameterizedCollection parametized, RecordField field) {
        if (parametized.isCollection()) {
            throw new RecordTypeConversionException(
                    "Nested collection in a collection is not supported in single level structure codification");
        }
        BiConsumer<RecordConsumer, Object> elemConsumer = null;
        if (parametized.isMap()) {
            ParameterizedMap parametizedChild = parametized.getParametizedAsMap();
            Consumer<Object> childWriter = createMapStructureWriter(parametizedChild, null);
            elemConsumer = (consumer, v) -> childWriter.accept(v);
        } else {
            Class<?> type = parametized.getActualType();
            elemConsumer = buildSimpleElementConsumer(type, recordConsumer, carpetConfiguration);
        }
        if (elemConsumer == null) {
            throw new RecordTypeConversionException("Unsuported type in collection");
        }
        return new OneLevelCollectionFieldWriter(recordConsumer, field, elemConsumer);
    }

    private Consumer<Object> createTwoLevelStructureWriter(ParameterizedCollection parametized,
            RecordField recordField) {
        BiConsumer<RecordConsumer, Object> elemConsumer = null;
        if (parametized.isCollection()) {
            ParameterizedCollection parametizedChild = parametized.getParametizedAsCollection();
            Consumer<Object> childWriter = createTwoLevelStructureWriter(parametizedChild, null);
            elemConsumer = (consumer, v) -> childWriter.accept(v);
        } else if (parametized.isMap()) {
            ParameterizedMap parametizedChild = parametized.getParametizedAsMap();
            Consumer<Object> childWriter = createMapStructureWriter(parametizedChild, null);
            elemConsumer = (consumer, v) -> childWriter.accept(v);
        } else {
            Class<?> type = parametized.getActualType();
            elemConsumer = buildSimpleElementConsumer(type, recordConsumer, carpetConfiguration);
        }
        if (elemConsumer == null) {
            throw new RecordTypeConversionException("Unsuported type in collection");
        }
        if (recordField != null) {
            return new TwoLevelCollectionRecordFieldWriter(recordConsumer, recordField, elemConsumer);
        }
        // We are referenced by other collection
        var innerStructureWriter = elemConsumer;
        return value -> {
            Collection<?> coll = (Collection<?>) value;
            if (coll != null) {
                writeGroupElementTwo(recordConsumer, innerStructureWriter, coll);
            }
        };
    }

    private Consumer<Object> createThreeLevelStructureWriter(ParameterizedCollection parametized,
            RecordField recordField) {
        BiConsumer<RecordConsumer, Object> elemConsumer = null;
        if (parametized.isCollection()) {
            ParameterizedCollection parametizedChild = parametized.getParametizedAsCollection();
            Consumer<Object> childWriter = createThreeLevelStructureWriter(parametizedChild, null);
            elemConsumer = (consumer, v) -> childWriter.accept(v);
        } else if (parametized.isMap()) {
            ParameterizedMap parametizedChild = parametized.getParametizedAsMap();
            Consumer<Object> childWriter = createMapStructureWriter(parametizedChild, null);
            elemConsumer = (consumer, v) -> childWriter.accept(v);
        } else {
            Class<?> type = parametized.getActualType();
            elemConsumer = buildSimpleElementConsumer(type, recordConsumer, carpetConfiguration);
        }
        if (elemConsumer == null) {
            throw new RecordTypeConversionException("Unsuported type in collection");
        }
        if (recordField != null) {
            return new ThreeLevelCollectionRecordFieldWriter(recordConsumer, recordField, elemConsumer);
        }
        // We are referenced by other collection
        var innerStructureWriter = elemConsumer;
        return value -> {
            Collection<?> coll = (Collection<?>) value;
            if (coll != null) {
                recordConsumer.startGroup();
                if (!coll.isEmpty()) {
                    writeGroupElementThree(recordConsumer, innerStructureWriter, coll);
                }
                recordConsumer.endGroup();
            }
        };
    }

    private Consumer<Object> createMapStructureWriter(ParameterizedMap parametized, RecordField recordField) {
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
            Consumer<Object> childWriter = createMapStructureWriter(parametizedChild, null);
            elemValueConsumer = (consumer, v) -> childWriter.accept(v);
        } else {
            Class<?> valueType = parametized.getValueActualType();
            elemValueConsumer = buildSimpleElementConsumer(valueType, recordConsumer, carpetConfiguration);
        }
        if (elemValueConsumer == null || elemKeyConsumer == null) {
            throw new RecordTypeConversionException("Unsuported type in Map");
        }
        if (recordField != null) {
            return new CollectionsWriters.MapRecordFieldWriter(recordConsumer, recordField, elemKeyConsumer,
                    elemValueConsumer);
        }
        // We are referenced by other collection
        var innerKeyStructureWriter = elemKeyConsumer;
        var innerValueStructureWriter = elemValueConsumer;
        return value -> {
            Map<?, ?> map = (Map<?, ?>) value;
            if (map != null) {
                recordConsumer.startGroup();
                if (!map.isEmpty()) {
                    writeKeyalueGroup(recordConsumer, innerKeyStructureWriter, innerValueStructureWriter, map);
                }
                recordConsumer.endGroup();
            }
        };
    }

    private static class FieldWriterConsumer implements Consumer<Object> {
        private final RecordConsumer recordConsumer;
        private final String fieldName;
        private final int idx;
        private final Function<Object, Object> accessor;
        private final BiConsumer<RecordConsumer, Object> writer;

        public FieldWriterConsumer(RecordConsumer recordConsumer, RecordField recordField,
                BiConsumer<RecordConsumer, Object> writer) {
            this.recordConsumer = recordConsumer;
            this.fieldName = recordField.fieldName();
            this.idx = recordField.idx();
            this.accessor = recordField.getAccessor();
            this.writer = writer;
        }

        @Override
        public void accept(Object object) {
            var value = accessor.apply(object);
            if (value != null) {
                recordConsumer.startField(fieldName, idx);
                writer.accept(recordConsumer, value);
                recordConsumer.endField(fieldName, idx);
            }
        }

    }
}
