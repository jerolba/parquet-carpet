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

import static com.jerolba.carpet.impl.write.CollectionsWriters.MapRecordFieldWriter.writeKeyalueGroup;
import static com.jerolba.carpet.impl.write.CollectionsWriters.ThreeLevelCollectionRecordFieldWriter.writeGroupElementThree;
import static com.jerolba.carpet.impl.write.CollectionsWriters.TwoLevelCollectionRecordFieldWriter.writeGroupElementTwo;
import static com.jerolba.carpet.impl.write.ModelFieldsWriter.buildPrimitiveJavaConsumer;
import static com.jerolba.carpet.impl.write.ModelFieldsWriter.buildSimpleElementConsumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.parquet.io.api.RecordConsumer;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.write.CollectionsWriters.OneLevelCollectionFieldWriter;
import com.jerolba.carpet.impl.write.CollectionsWriters.ThreeLevelCollectionRecordFieldWriter;
import com.jerolba.carpet.impl.write.CollectionsWriters.TwoLevelCollectionRecordFieldWriter;
import com.jerolba.carpet.model.CollectionType;
import com.jerolba.carpet.model.FieldType;
import com.jerolba.carpet.model.MapType;
import com.jerolba.carpet.model.WriteField;
import com.jerolba.carpet.model.WriteRecordModelType;
import com.jerolba.carpet.model.WriteRecordModelType.FunctionFieldInfo;
import com.jerolba.carpet.model.WriteRecordModelType.PrimitiveJavaFieldInfo;

class WriteRecordModelWriter {

    private final RecordConsumer recordConsumer;
    private final CarpetWriteConfiguration carpetConfiguration;

    private final List<Consumer<Object>> fieldWriters = new ArrayList<>();

    public WriteRecordModelWriter(RecordConsumer recordConsumer, WriteRecordModelType<?> writeRecordModelType,
            CarpetWriteConfiguration carpetConfiguration) {
        this.recordConsumer = recordConsumer;
        this.carpetConfiguration = carpetConfiguration;

        int idx = 0;
        for (WriteField<?> field : writeRecordModelType.getFields()) {
            Consumer<Object> writer = buildFieldWriter(recordConsumer, idx, field);
            if (writer == null) {
                throw new RuntimeException(field.fieldType().getClass().getName() + " can not be serialized");
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

    private Consumer<Object> buildFieldWriter(RecordConsumer recordConsumer, int idx, WriteField<?> field) {
        if (field instanceof PrimitiveJavaFieldInfo<?> primitiveJavaField) {
            return buildPrimitiveJavaConsumer(primitiveJavaField, idx, recordConsumer);
        }
        if (field instanceof FunctionFieldInfo<?> functionField) {
            Function<Object, Object> accessor = (Function<Object, Object>) functionField.accessor();
            RecordField modelField = new WriteModelField(field.parquetFieldName(), idx, accessor);
            FieldType type = field.fieldType();
            if (type instanceof CollectionType collectionType) {
                return createCollectionWriter(collectionType, modelField);
            } else if (type instanceof MapType mapType) {
                return createMapStructureWriter(mapType, modelField);
            }
            BiConsumer<RecordConsumer, Object> basicTypeWriter = buildSimpleElementConsumer(type, recordConsumer,
                    carpetConfiguration);
            if (basicTypeWriter != null) {
                return new FieldWriterConsumer(recordConsumer, modelField, basicTypeWriter);
            }
        }
        return null;
    }

    private Consumer<Object> createCollectionWriter(CollectionType collectionType, RecordField recordField) {
        return switch (carpetConfiguration.annotatedLevels()) {
        case ONE -> createOneLevelStructureWriter(collectionType.type(), recordField);
        case TWO -> createTwoLevelStructureWriter(collectionType.type(), recordField);
        case THREE -> createThreeLevelStructureWriter(collectionType.type(), recordField);
        };
    }

    private Consumer<Object> createOneLevelStructureWriter(FieldType parametized, RecordField recordField) {
        if (parametized instanceof CollectionType) {
            throw new RecordTypeConversionException(
                    "Nested collection in a collection is not supported in single level structure codification");
        }
        BiConsumer<RecordConsumer, Object> elemConsumer = null;
        if (parametized instanceof MapType mapType) {
            Consumer<Object> childWriter = createMapStructureWriter(mapType, null);
            elemConsumer = (consumer, v) -> childWriter.accept(v);
        } else {
            elemConsumer = buildSimpleElementConsumer(parametized, recordConsumer, carpetConfiguration);
        }
        if (elemConsumer == null) {
            throw new RecordTypeConversionException("Unsuported type in collection");
        }
        return new OneLevelCollectionFieldWriter(recordConsumer, recordField, elemConsumer);
    }

    private Consumer<Object> createTwoLevelStructureWriter(FieldType parametized, RecordField recordField) {
        BiConsumer<RecordConsumer, Object> elemConsumer = null;
        if (parametized instanceof CollectionType collectionType) {
            Consumer<Object> childWriter = createTwoLevelStructureWriter(collectionType.type(), null);
            elemConsumer = (consumer, v) -> childWriter.accept(v);
        } else if (parametized instanceof MapType mapType) {
            Consumer<Object> childWriter = createMapStructureWriter(mapType, null);
            elemConsumer = (consumer, v) -> childWriter.accept(v);
        } else {
            elemConsumer = buildSimpleElementConsumer(parametized, recordConsumer, carpetConfiguration);
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

    private Consumer<Object> createThreeLevelStructureWriter(FieldType parametized, RecordField recordField) {
        BiConsumer<RecordConsumer, Object> elemConsumer = null;
        if (parametized instanceof CollectionType collectionType) {
            Consumer<Object> childWriter = createThreeLevelStructureWriter(collectionType.type(), null);
            elemConsumer = (consumer, v) -> childWriter.accept(v);
        } else if (parametized instanceof MapType mapType) {
            Consumer<Object> childWriter = createMapStructureWriter(mapType, null);
            elemConsumer = (consumer, v) -> childWriter.accept(v);
        } else {
            elemConsumer = buildSimpleElementConsumer(parametized, recordConsumer, carpetConfiguration);
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

    private Consumer<Object> createMapStructureWriter(MapType mapType, RecordField recordField) {
        // Key
        BiConsumer<RecordConsumer, Object> elemKeyConsumer = null;
        FieldType keyType = mapType.keyType();
        elemKeyConsumer = buildSimpleElementConsumer(keyType, recordConsumer, carpetConfiguration);

        // Value
        BiConsumer<RecordConsumer, Object> elemValueConsumer = null;
        if (mapType.valueType() instanceof CollectionType valueCollectionType) {
            Consumer<Object> childWriter = createCollectionWriter(valueCollectionType, null);
            elemValueConsumer = (consumer, v) -> childWriter.accept(v);
        } else if (mapType.valueType() instanceof MapType valueMapType) {
            Consumer<Object> childWriter = createMapStructureWriter(valueMapType, null);
            elemValueConsumer = (consumer, v) -> childWriter.accept(v);
        } else {
            elemValueConsumer = buildSimpleElementConsumer(mapType.valueType(), recordConsumer, carpetConfiguration);
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
            if (value != null) {
                Map<?, ?> map = (Map<?, ?>) value;
                recordConsumer.startGroup();
                if (!map.isEmpty()) {
                    writeKeyalueGroup(recordConsumer, innerKeyStructureWriter, innerValueStructureWriter, map);
                }
                recordConsumer.endGroup();
            }
        };
    }
}
