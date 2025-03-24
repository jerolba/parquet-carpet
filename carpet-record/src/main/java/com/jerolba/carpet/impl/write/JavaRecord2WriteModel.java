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

import static com.jerolba.carpet.impl.NotNullField.isNotNull;
import static com.jerolba.carpet.model.FieldTypes.LIST;
import static com.jerolba.carpet.model.FieldTypes.MAP;
import static com.jerolba.carpet.model.FieldTypes.writeRecordModel;

import java.lang.reflect.TypeVariable;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.JavaType;
import com.jerolba.carpet.impl.Parameterized;
import com.jerolba.carpet.impl.ParameterizedCollection;
import com.jerolba.carpet.impl.ParameterizedMap;
import com.jerolba.carpet.model.EnumType;
import com.jerolba.carpet.model.FieldType;
import com.jerolba.carpet.model.FieldTypes;
import com.jerolba.carpet.model.WriteRecordModelType;

public class JavaRecord2WriteModel {

    private final com.jerolba.carpet.impl.write.FieldToColumnMapper fieldToColumnMapper;

    public JavaRecord2WriteModel(CarpetWriteConfiguration carpetConfiguration) {
        this.fieldToColumnMapper = new com.jerolba.carpet.impl.write.FieldToColumnMapper(
                carpetConfiguration.columnNamingStrategy());
    }

    public <T> WriteRecordModelType<T> createModel(Class<T> recordClass) {
        return buildRecordModel(recordClass, new HashSet<>());
    }

    private <T> WriteRecordModelType<T> buildRecordModel(Class<T> classToModel, Set<Class<?>> visited) {
        visited = validateNotVisitedRecord(classToModel, visited);

        WriteRecordModelType<T> writeModel = writeRecordModel(classToModel);
        createRecordFields(writeModel, classToModel, visited);
        return writeModel;
    }

    private <T> void createRecordFields(WriteRecordModelType<T> writeModel, Class<T> recordClass,
            Set<Class<?>> visited) {

        for (var attr : recordClass.getRecordComponents()) {
            java.lang.reflect.Type genericType = attr.getGenericType();
            if (genericType instanceof TypeVariable<?>) {
                throw new RecordTypeConversionException(genericType.toString() + " generic types not supported");
            }
            Class<?> type = attr.getType();
            JavaType javaType = new JavaType(type);
            FieldType fieldType = null;
            if (javaType.isCollection()) {
                var parameterizedCollection = Parameterized.getParameterizedCollection(attr);
                fieldType = createCollectionType(parameterizedCollection, visited);
            } else if (javaType.isMap()) {
                var parameterizedMap = Parameterized.getParameterizedMap(attr);
                fieldType = createMapType(parameterizedMap, visited);
            } else {
                boolean notNull = type.isPrimitive() || isNotNull(attr);
                fieldType = simpleOrCompositeClass(type, notNull, visited);
            }
            String fieldName = fieldToColumnMapper.getColumnName(attr);
            Function<T, Object> recordAccessor = (Function<T, Object>) Reflection.recordAccessor(recordClass, attr);
            writeModel.withField(fieldName, fieldType, recordAccessor);
        }
    }

    private FieldType simpleOrCompositeClass(Class<?> type, boolean isPrimitive, Set<Class<?>> visited) {
        FieldType simple = buildSimpleType(type, isPrimitive);
        return simple == null ? buildRecordModel(type, visited) : simple;
    }

    private FieldType createCollectionType(ParameterizedCollection parametized, Set<Class<?>> visited) {
        if (parametized.isCollection()) {
            return LIST.ofType(createCollectionType(parametized.getParametizedAsCollection(), visited));
        } else if (parametized.isMap()) {
            return LIST.ofType(createMapType(parametized.getParametizedAsMap(), visited));
        }
        return LIST.ofType(simpleOrCompositeClass(parametized.getActualType(), false, visited));
    }

    private FieldType createMapType(ParameterizedMap parametized, Set<Class<?>> visited) {
        Class<?> keyType = parametized.getKeyActualType();
        if (parametized.keyIsCollection() || parametized.keyIsMap()) {
            throw new RuntimeException("Maps with collections or maps as keys are not supported");
        }
        FieldType nestedKey = simpleOrCompositeClass(keyType, false, visited);

        if (parametized.valueIsCollection()) {
            FieldType childCollection = createCollectionType(parametized.getValueTypeAsCollection(), visited);
            return MAP.ofTypes(nestedKey, childCollection);
        } else if (parametized.valueIsMap()) {
            FieldType childMap = createMapType(parametized.getValueTypeAsMap(), visited);
            return MAP.ofTypes(nestedKey, childMap);
        }
        FieldType nestedValue = simpleOrCompositeClass(parametized.getValueActualType(), false, visited);
        if (nestedKey != null && nestedValue != null) {
            return MAP.ofTypes(nestedKey, nestedValue);
        }
        throw new RecordTypeConversionException("Unsuported type in Map");
    }

    private Set<Class<?>> validateNotVisitedRecord(Class<?> recordClass, Set<Class<?>> visited) {
        if (!recordClass.isRecord()) {
            throw new RecordTypeConversionException(recordClass.getName() + " must be a java Record");
        }
        if (visited.contains(recordClass)) {
            throw new RecordTypeConversionException("Recusive classes are not supported");
        }
        visited = new HashSet<>(visited);
        visited.add(recordClass);
        return visited;
    }

    public static FieldType buildSimpleType(Class<?> type, boolean isPrimitive) {
        JavaType javaType = new JavaType(type);
        FieldType javaPrimitiveType = javaPrimitiveTypes(javaType, isPrimitive);
        return javaPrimitiveType != null ? javaPrimitiveType : javaTypes(type, javaType);
    }

    private static FieldType javaPrimitiveTypes(JavaType javaType, boolean isPrimitive) {
        if (javaType.isInteger()) {
            return isPrimitive ? FieldTypes.INTEGER.notNull() : FieldTypes.INTEGER;
        }
        if (javaType.isLong()) {
            return isPrimitive ? FieldTypes.LONG.notNull() : FieldTypes.LONG;
        }
        if (javaType.isFloat()) {
            return isPrimitive ? FieldTypes.FLOAT.notNull() : FieldTypes.FLOAT;
        }
        if (javaType.isDouble()) {
            return isPrimitive ? FieldTypes.DOUBLE.notNull() : FieldTypes.DOUBLE;
        }
        if (javaType.isBoolean()) {
            return isPrimitive ? FieldTypes.BOOLEAN.notNull() : FieldTypes.BOOLEAN;
        }
        if (javaType.isShort()) {
            return isPrimitive ? FieldTypes.SHORT.notNull() : FieldTypes.SHORT;
        }
        if (javaType.isByte()) {
            return isPrimitive ? FieldTypes.BYTE.notNull() : FieldTypes.BYTE;
        }
        return null;
    }

    private static FieldType javaTypes(Class<?> type, JavaType javaType) {
        if (javaType.isString()) {
            return FieldTypes.STRING;
        }
        if (javaType.isEnum()) {
            return new EnumType(false, (Class<? extends Enum<?>>) type);
        }
        if (javaType.isUuid()) {
            return FieldTypes.UUID;
        }
        if (javaType.isBigDecimal()) {
            return FieldTypes.BIG_DECIMAL;
        }
        if (javaType.isLocalDate()) {
            return FieldTypes.LOCAL_DATE;
        }
        if (javaType.isLocalTime()) {
            return FieldTypes.LOCAL_TIME;
        }
        if (javaType.isLocalDateTime()) {
            return FieldTypes.LOCAL_DATE_TIME;
        }
        if (javaType.isInstant()) {
            return FieldTypes.INSTANT;
        }
        return null;
    }

}