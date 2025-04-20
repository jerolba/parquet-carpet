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
import com.jerolba.carpet.annotation.ParquetBson;
import com.jerolba.carpet.annotation.ParquetJson;
import com.jerolba.carpet.annotation.ParquetString;
import com.jerolba.carpet.impl.JavaType;
import com.jerolba.carpet.impl.Parameterized;
import com.jerolba.carpet.impl.ParameterizedCollection;
import com.jerolba.carpet.impl.ParameterizedMap;
import com.jerolba.carpet.model.BinaryType;
import com.jerolba.carpet.model.EnumType;
import com.jerolba.carpet.model.FieldType;
import com.jerolba.carpet.model.FieldTypes;
import com.jerolba.carpet.model.StringType;
import com.jerolba.carpet.model.WriteRecordModelType;

public class JavaRecord2WriteModel {

    private final com.jerolba.carpet.impl.write.FieldToColumnMapper fieldToColumnMapper;

    public JavaRecord2WriteModel(CarpetWriteConfiguration carpetConfiguration) {
        this.fieldToColumnMapper = new com.jerolba.carpet.impl.write.FieldToColumnMapper(
                carpetConfiguration.columnNamingStrategy());
    }

    public <T> WriteRecordModelType<T> createModel(Class<T> recordClass) {
        return buildRecordModel(recordClass, false, new HashSet<>());
    }

    private <T> WriteRecordModelType<T> buildRecordModel(Class<T> classToModel, boolean isNotNull,
            Set<Class<?>> visited) {
        visited = validateNotVisitedRecord(classToModel, visited);

        WriteRecordModelType<T> writeModel = writeRecordModel(classToModel);
        createRecordFields(writeModel, classToModel, visited);
        return isNotNull ? writeModel.notNull() : writeModel;
    }

    private <T> void createRecordFields(WriteRecordModelType<T> writeModel, Class<T> recordClass,
            Set<Class<?>> visited) {

        for (var attr : recordClass.getRecordComponents()) {
            java.lang.reflect.Type genericType = attr.getGenericType();
            if (genericType instanceof TypeVariable<?>) {
                throw new RecordTypeConversionException(genericType.toString() + " generic types not supported");
            }
            Class<?> type = attr.getType();
            JavaType javaType = new JavaType(type, attr.getDeclaredAnnotations());
            FieldType fieldType = null;
            if (javaType.isCollection()) {
                var parameterizedCollection = Parameterized.getParameterizedCollection(attr);
                fieldType = createCollectionType(parameterizedCollection, visited);
            } else if (javaType.isMap()) {
                var parameterizedMap = Parameterized.getParameterizedMap(attr);
                fieldType = createMapType(parameterizedMap, visited);
            } else {
                boolean notNull = type.isPrimitive() || isNotNull(attr);
                fieldType = simpleOrCompositeClass(javaType, notNull, visited);
            }
            String fieldName = fieldToColumnMapper.getColumnName(attr);
            Function<T, Object> recordAccessor = (Function<T, Object>) Reflection.recordAccessor(recordClass, attr);
            writeModel.withField(fieldName, fieldType, recordAccessor);
        }
    }

    private FieldType simpleOrCompositeClass(JavaType javaType, boolean isNotNull, Set<Class<?>> visited) {
        FieldType simple = buildSimpleType(javaType, isNotNull);
        return simple == null ? buildRecordModel(javaType.getJavaType(), isNotNull, visited) : simple;
    }

    private FieldType createCollectionType(ParameterizedCollection parametized, Set<Class<?>> visited) {
        if (parametized.isCollection()) {
            return LIST.ofType(createCollectionType(parametized.getParametizedAsCollection(), visited));
        } else if (parametized.isMap()) {
            return LIST.ofType(createMapType(parametized.getParametizedAsMap(), visited));
        }
        return LIST.ofType(simpleOrCompositeClass(new JavaType(parametized.getActualType()), false, visited));
    }

    private FieldType createMapType(ParameterizedMap parametized, Set<Class<?>> visited) {
        Class<?> keyType = parametized.getKeyActualType();
        if (parametized.keyIsCollection() || parametized.keyIsMap()) {
            throw new RuntimeException("Maps with collections or maps as keys are not supported");
        }
        FieldType nestedKey = simpleOrCompositeClass(new JavaType(keyType), false, visited);

        if (parametized.valueIsCollection()) {
            FieldType childCollection = createCollectionType(parametized.getValueTypeAsCollection(), visited);
            return MAP.ofTypes(nestedKey, childCollection);
        } else if (parametized.valueIsMap()) {
            FieldType childMap = createMapType(parametized.getValueTypeAsMap(), visited);
            return MAP.ofTypes(nestedKey, childMap);
        }
        FieldType nestedValue = simpleOrCompositeClass(new JavaType(parametized.getValueActualType()), false, visited);
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

    public static FieldType buildSimpleType(JavaType javaType, boolean isNotNull) {
        FieldType javaPrimitiveType = javaPrimitiveTypes(javaType, isNotNull);
        return javaPrimitiveType != null ? javaPrimitiveType : javaTypes(javaType, isNotNull);
    }

    private static FieldType javaPrimitiveTypes(JavaType javaType, boolean isNotNull) {
        if (javaType.isInteger()) {
            return isNotNull ? FieldTypes.INTEGER.notNull() : FieldTypes.INTEGER;
        }
        if (javaType.isLong()) {
            return isNotNull ? FieldTypes.LONG.notNull() : FieldTypes.LONG;
        }
        if (javaType.isFloat()) {
            return isNotNull ? FieldTypes.FLOAT.notNull() : FieldTypes.FLOAT;
        }
        if (javaType.isDouble()) {
            return isNotNull ? FieldTypes.DOUBLE.notNull() : FieldTypes.DOUBLE;
        }
        if (javaType.isBoolean()) {
            return isNotNull ? FieldTypes.BOOLEAN.notNull() : FieldTypes.BOOLEAN;
        }
        if (javaType.isShort()) {
            return isNotNull ? FieldTypes.SHORT.notNull() : FieldTypes.SHORT;
        }
        if (javaType.isByte()) {
            return isNotNull ? FieldTypes.BYTE.notNull() : FieldTypes.BYTE;
        }
        return null;
    }

    private static FieldType javaTypes(JavaType javaType, boolean isNotNull) {
        if (javaType.isString()) {
            StringType type = isNotNull ? FieldTypes.STRING.notNull() : FieldTypes.STRING;
            if (javaType.isAnnotatedWith(ParquetJson.class)) {
                type = type.asJson();
            }
            return type;
        }
        if (javaType.isBinary()) {
            if (javaType.isAnnotatedWith(ParquetString.class)) {
                BinaryType binary = FieldTypes.BINARY.asString();
                return isNotNull ? binary.notNull() : binary;
            } else if (javaType.isAnnotatedWith(ParquetJson.class)) {
                BinaryType binary = FieldTypes.BINARY.asJson();
                return isNotNull ? binary.notNull() : binary;
            } else if (javaType.isAnnotatedWith(ParquetBson.class)) {
                BinaryType binary = FieldTypes.BINARY.asBson();
                return isNotNull ? binary.notNull() : binary;
            }
            throw new RecordTypeConversionException(
                    "Binary must be annotated with the type of Parquet LogicalType to use");

        }
        if (javaType.isEnum()) {
            if (javaType.isAnnotatedWith(ParquetString.class)) {
                BinaryType binary = FieldTypes.BINARY.asString();
                return isNotNull ? binary.notNull() : binary;
            }
            EnumType enumType = new EnumType(false, (Class<? extends Enum<?>>) javaType.getJavaType());
            return isNotNull ? enumType.notNull() : enumType;
        }
        if (javaType.isUuid()) {
            return isNotNull ? FieldTypes.UUID.notNull() : FieldTypes.UUID;
        }
        if (javaType.isBigDecimal()) {
            return isNotNull ? FieldTypes.BIG_DECIMAL.notNull() : FieldTypes.BIG_DECIMAL;
        }
        if (javaType.isLocalDate()) {
            return isNotNull ? FieldTypes.LOCAL_DATE.notNull() : FieldTypes.LOCAL_DATE;
        }
        if (javaType.isLocalTime()) {
            return isNotNull ? FieldTypes.LOCAL_TIME.notNull() : FieldTypes.LOCAL_TIME;
        }
        if (javaType.isLocalDateTime()) {
            return isNotNull ? FieldTypes.LOCAL_DATE_TIME.notNull() : FieldTypes.LOCAL_DATE_TIME;
        }
        if (javaType.isInstant()) {
            return isNotNull ? FieldTypes.INSTANT.notNull() : FieldTypes.INSTANT;
        }
        return null;
    }

}