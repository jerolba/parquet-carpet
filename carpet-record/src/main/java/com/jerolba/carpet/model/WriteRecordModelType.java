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
package com.jerolba.carpet.model;

import static com.jerolba.carpet.model.FieldTypes.BOOLEAN;
import static com.jerolba.carpet.model.FieldTypes.BYTE;
import static com.jerolba.carpet.model.FieldTypes.DOUBLE;
import static com.jerolba.carpet.model.FieldTypes.FLOAT;
import static com.jerolba.carpet.model.FieldTypes.INTEGER;
import static com.jerolba.carpet.model.FieldTypes.LONG;
import static com.jerolba.carpet.model.FieldTypes.SHORT;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

import com.jerolba.carpet.RecordTypeConversionException;

public final class WriteRecordModelType<T> implements FieldType {

    private final Class<T> recordClass;
    private final List<WriteField<T>> fields = new ArrayList<>();
    private final Map<String, WriteField<T>> indexedFields = new HashMap<>();
    private boolean isNotNull = false;
    private Integer fieldId = null;
    private final Set<Integer> nestedFieldIds = new HashSet<>();

    WriteRecordModelType(Class<T> recordClass) {
        this.recordClass = recordClass;
    }

    static <T> WriteRecordModelType<T> writeRecordModel(Class<T> recordClass) {
        return new WriteRecordModelType<>(recordClass);
    }

    @Override
    public Class<T> getClassType() {
        return recordClass;
    }

    public WriteRecordModelType<T> notNull() {
        isNotNull = true;
        return this;
    }

    public WriteRecordModelType<T> fieldId(Integer fieldId) {
        this.fieldId = fieldId;
        return this;
    }

    @Override
    public boolean isNotNull() {
        return isNotNull;
    }

    @Override
    public Integer fieldId() {
        return fieldId;
    }

    public List<WriteField<T>> getFields() {
        return fields;
    }

    public WriteRecordModelType<T> withField(String parquetFieldName, FieldType type, Function<T, Object> accessor) {
        requireNonNull(parquetFieldName);
        requireNonNull(type);
        requireNonNull(accessor);
        if (indexedFields.containsKey(parquetFieldName)) {
            throw new IllegalArgumentException(parquetFieldName + " already defined");
        }
        addNestedFieldId(type);
        var field = new FunctionFieldInfo<>(parquetFieldName, type, accessor);
        fields.add(field);
        indexedFields.put(parquetFieldName, field);
        return this;
    }

    public WriteRecordModelType<T> withField(String parquetFieldName, ToBooleanFunction<T> accessor) {
        return withPrimitiveField(parquetFieldName, BOOLEAN.notNull(), accessor);
    }

    public WriteRecordModelType<T> withField(String parquetFieldName, ToByteFunction<T> accessor) {
        return withPrimitiveField(parquetFieldName, BYTE.notNull(), accessor);
    }

    public WriteRecordModelType<T> withField(String parquetFieldName, ToShortFunction<T> accessor) {
        return withPrimitiveField(parquetFieldName, SHORT.notNull(), accessor);
    }

    public WriteRecordModelType<T> withField(String parquetFieldName, ToIntFunction<T> accessor) {
        return withPrimitiveField(parquetFieldName, INTEGER.notNull(), accessor);
    }

    public WriteRecordModelType<T> withField(String parquetFieldName, ToLongFunction<T> accessor) {
        return withPrimitiveField(parquetFieldName, LONG.notNull(), accessor);
    }

    public WriteRecordModelType<T> withField(String parquetFieldName, ToFloatFunction<T> accessor) {
        return withPrimitiveField(parquetFieldName, FLOAT.notNull(), accessor);
    }

    public WriteRecordModelType<T> withField(String parquetFieldName, ToDoubleFunction<T> accessor) {
        return withPrimitiveField(parquetFieldName, DOUBLE.notNull(), accessor);
    }

    public WriteRecordModelType<T> withPrimitiveField(String parquetFieldName, FieldType type, Object accessor) {
        requireNonNull(parquetFieldName);
        requireNonNull(type);
        requireNonNull(accessor);
        if (indexedFields.containsKey(parquetFieldName)) {
            throw new IllegalArgumentException(parquetFieldName + " already defined");
        }
        if (!type.isNotNull()) {
            throw new IllegalArgumentException(parquetFieldName + " is not defined as not null");
        }
        addNestedFieldId(type);
        WriteField<T> field = new PrimitiveJavaFieldInfo<>(parquetFieldName, type, accessor);
        fields.add(field);
        indexedFields.put(parquetFieldName, field);
        return this;
    }

    public record PrimitiveJavaFieldInfo<T>(String parquetFieldName, FieldType fieldType, Object accessor)
            implements WriteField<T> {
    }

    public record FunctionFieldInfo<T>(String parquetFieldName, FieldType fieldType, Function<T, Object> accessor)
            implements WriteField<T> {
    }

    private void addNestedFieldId(FieldType fieldType) {
        Integer fieldId = fieldType.fieldId();
        if (fieldId == null) {
            return;
        }
        if (nestedFieldIds.contains(fieldId)) {
            throw new RecordTypeConversionException(
                    "Duplicate field ID " + fieldId + " found in record " + recordClass.getSimpleName() +
                            ". Field IDs must be unique within the same record scope.");
        }
        nestedFieldIds.add(fieldId);
    }
}