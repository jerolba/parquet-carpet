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
import static com.jerolba.carpet.impl.write.CollectionsWriters.mapRecordFieldWriterFactory;
import static com.jerolba.carpet.impl.write.CollectionsWriters.threeLevelCollectionRecordFieldWriterFactory;
import static com.jerolba.carpet.impl.write.CollectionsWriters.twoLevelCollectionRecordFieldWriterFactory;
import static com.jerolba.carpet.impl.write.FieldsWriter.buildPrimitiveAccessor;
import static com.jerolba.carpet.impl.write.FieldsWriter.buildPrimitiveJavaConsumer;
import static com.jerolba.carpet.impl.write.FieldsWriter.buildSimpleElementConsumer;

import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.parquet.io.api.RecordConsumer;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.JavaType;
import com.jerolba.carpet.impl.ParameterizedCollection;
import com.jerolba.carpet.impl.ParameterizedMap;
import com.jerolba.carpet.impl.write.CollectionsWriters.OneLevelCollectionFieldWriter;

class CarpetRecordWriter {

    private final RecordConsumer recordConsumer;
    private final CarpetWriteConfiguration carpetConfiguration;

    private final List<Consumer<Object>> fieldWriters = new ArrayList<>();

    public CarpetRecordWriter(RecordConsumer recordConsumer, Class<?> recordClass,
            CarpetWriteConfiguration carpetConfiguration) {
        this.recordConsumer = recordConsumer;
        this.carpetConfiguration = carpetConfiguration;

        int idx = 0;
        for (RecordComponent attr : recordClass.getRecordComponents()) {
            JavaType javaType = new JavaType(attr);
            String parquetFieldName = getFieldName(attr);
            Consumer<Object> writer = null;
            if (javaType.isJavaPrimitive()) {
                var accessor = buildPrimitiveAccessor(recordClass, attr, javaType);
                if (accessor != null) {
                    writer = buildPrimitiveJavaConsumer(parquetFieldName, javaType, accessor, idx, recordConsumer);
                }
            }
            if (writer == null) {
                Class<?> type = attr.getType();
                RecordField f = new ReflectionRecordField(recordClass, attr, parquetFieldName, idx);
                BiConsumer<RecordConsumer, Object> basicTypeWriter = buildSimpleElementConsumer(javaType,
                        recordConsumer, carpetConfiguration);
                if (basicTypeWriter != null) {
                    writer = new FieldWriterConsumer(recordConsumer, f, basicTypeWriter);
                } else if (Collection.class.isAssignableFrom(type)) {
                    writer = createCollectionWriter(getParameterizedCollection(attr), f);
                } else if (Map.class.isAssignableFrom(type)) {
                    writer = createMapStructureWriter(getParameterizedMap(attr), f);
                } else {
                    throw new RuntimeException(type.getName() + " can not be serialized");
                }
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
            ParameterizedMap parametizedChild = parametized.getAsMap();
            Consumer<Object> childWriter = createMapStructureWriter(parametizedChild, null);
            elemConsumer = (consumer, v) -> childWriter.accept(v);
        } else {
            JavaType type = parametized.getActualJavaType();
            elemConsumer = buildSimpleElementConsumer(type, recordConsumer, carpetConfiguration);
        }
        if (elemConsumer == null) {
            throw new RecordTypeConversionException("Unsuported type in collection");
        }
        return new OneLevelCollectionFieldWriter(recordConsumer, field, elemConsumer);
    }

    private Consumer<Object> createTwoLevelStructureWriter(ParameterizedCollection parametized,
            RecordField recordField) {
        BiConsumer<RecordConsumer, Object> elemConsumer = buildCollectionWriter(parametized);
        if (elemConsumer == null) {
            throw new RecordTypeConversionException("Unsuported type in collection");
        }
        return twoLevelCollectionRecordFieldWriterFactory(recordConsumer, recordField, elemConsumer);
    }

    private Consumer<Object> createThreeLevelStructureWriter(ParameterizedCollection generic,
            RecordField recordField) {
        BiConsumer<RecordConsumer, Object> elemConsumer = buildCollectionWriter(generic);
        if (elemConsumer == null) {
            throw new RecordTypeConversionException("Unsuported type in collection");
        }
        return threeLevelCollectionRecordFieldWriterFactory(recordConsumer, recordField, elemConsumer);
    }

    private BiConsumer<RecordConsumer, Object> buildCollectionWriter(ParameterizedCollection generic) {
        if (generic.isCollection()) {
            Consumer<Object> childWriter = createCollectionWriter(generic.getAsCollection(), null);
            return (consumer, v) -> childWriter.accept(v);
        }
        if (generic.isMap()) {
            Consumer<Object> childWriter = createMapStructureWriter(generic.getAsMap(), null);
            return (consumer, v) -> childWriter.accept(v);
        }
        return buildSimpleElementConsumer(generic.getActualJavaType(), recordConsumer, carpetConfiguration);
    }

    private Consumer<Object> createMapStructureWriter(ParameterizedMap generic, RecordField recordField) {
        JavaType keyType = generic.getGenericKey().getActualJavaType();
        BiConsumer<RecordConsumer, Object> keyConsumer = buildSimpleElementConsumer(keyType, recordConsumer,
                carpetConfiguration);

        BiConsumer<RecordConsumer, Object> valueConsumer = buildCollectionWriter(generic.getGenericValue());
        return mapRecordFieldWriterFactory(recordConsumer, recordField, keyConsumer, valueConsumer);
    }
}
