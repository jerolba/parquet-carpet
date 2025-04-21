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
import static com.jerolba.carpet.impl.Parameterized.getParameterizedCollection;
import static com.jerolba.carpet.impl.Parameterized.getParameterizedMap;
import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timeType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.uuidType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.apache.parquet.schema.Types.primitive;

import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.parquet.schema.ConversionPatterns;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.TimeUnit;
import com.jerolba.carpet.impl.JavaType;
import com.jerolba.carpet.impl.ParameterizedCollection;
import com.jerolba.carpet.impl.ParameterizedMap;

class JavaRecord2Schema {

    private final CarpetWriteConfiguration carpetConfiguration;
    private final FieldToColumnMapper fieldToColumnMapper;

    public JavaRecord2Schema(CarpetWriteConfiguration carpetConfiguration) {
        this.carpetConfiguration = carpetConfiguration;
        this.fieldToColumnMapper = new FieldToColumnMapper(carpetConfiguration.columnNamingStrategy());
    }

    public MessageType createSchema(Class<?> recordClass) {
        return build(recordClass, new HashSet<>());
    }

    private MessageType build(Class<?> recordClass, Set<Class<?>> visited) {
        validateNotVisitedRecord(recordClass, visited);
        String groupName = recordClass.getSimpleName();

        List<Type> fields = createGroupFields(recordClass, visited);
        return new MessageType(groupName, fields);
    }

    private List<Type> createGroupFields(Class<?> recordClass, Set<Class<?>> visited) {
        List<Type> fields = new ArrayList<>();
        for (var attr : recordClass.getRecordComponents()) {
            Class<?> type = attr.getType();
            String fieldName = fieldToColumnMapper.getColumnName(attr);
            boolean notNull = type.isPrimitive() || isNotNull(attr);
            Repetition repetition = notNull ? REQUIRED : OPTIONAL;
            JavaType javaType = new JavaType(type);

            Type parquetType = buildType(type, visited, repetition, fieldName);
            if (parquetType != null) {
                fields.add(parquetType);
            } else if (javaType.isCollection()) {
                var parameterizedCollection = getParameterizedCollection(attr);
                fields.add(createCollectionType(fieldName, parameterizedCollection, visited, repetition));
            } else if (javaType.isMap()) {
                var parameterizedMap = getParameterizedMap(attr);
                fields.add(createMapType(fieldName, parameterizedMap, visited, repetition));
            } else {
                java.lang.reflect.Type genericType = attr.getGenericType();
                if (genericType instanceof TypeVariable<?>) {
                    throw new RecordTypeConversionException(genericType.toString() + " generic types not supported");
                }
                throw new RecordTypeConversionException(
                        "Field '" + attr.getName() + "' of type " + attr.getType() + " not supported");
            }
        }
        return fields;
    }

    private List<Type> buildCompositeChild(Class<?> recordClass, Set<Class<?>> visited) {
        validateNotVisitedRecord(recordClass, visited);
        List<Type> fields = createGroupFields(recordClass, visited);
        visited.remove(recordClass);
        return fields;
    }

    private Type createCollectionType(String fieldName, ParameterizedCollection collectionClass, Set<Class<?>> visited,
            Repetition repetition) {
        return switch (carpetConfiguration.annotatedLevels()) {
        case ONE -> createCollectionOneLevel(fieldName, collectionClass, visited);
        case TWO -> createCollectionTwoLevel(fieldName, collectionClass, visited, repetition);
        case THREE -> createCollectionThreeLevel(fieldName, collectionClass, visited, repetition);
        };
    }

    private Type createCollectionOneLevel(String fieldName, ParameterizedCollection parametized,
            Set<Class<?>> visited) {
        if (parametized.isCollection()) {
            throw new RecordTypeConversionException(
                    "Recursive collections not supported in annotated 1-level structures");
        }
        if (parametized.isMap()) {
            return createMapType(fieldName, parametized.getParametizedAsMap(), visited, REPEATED);
        }
        return buildTypeElement(parametized.getActualType(), visited, REPEATED, fieldName);
    }

    private Type createCollectionTwoLevel(String fieldName, ParameterizedCollection parametized, Set<Class<?>> visited,
            Repetition repetition) {
        // Two level collections elements are not nullables
        Type nested = createNestedCollection(parametized, visited, REPEATED);
        return ConversionPatterns.listType(repetition, fieldName, nested);
    }

    private Type createCollectionThreeLevel(String fieldName, ParameterizedCollection parametized,
            Set<Class<?>> visited, Repetition repetition) {
        // Three level collections elements are nullables
        Type nested = createNestedCollection(parametized, visited, OPTIONAL);
        return ConversionPatterns.listOfElements(repetition, fieldName, nested);
    }

    private Type createNestedCollection(ParameterizedCollection parametized, Set<Class<?>> visited,
            Repetition repetition) {
        if (parametized.isCollection()) {
            return createCollectionType("element", parametized.getParametizedAsCollection(), visited, repetition);
        }
        if (parametized.isMap()) {
            return createMapType("element", parametized.getParametizedAsMap(), visited, repetition);
        }
        return buildTypeElement(parametized.getActualType(), visited, repetition, "element");
    }

    private Type createMapType(String fieldName, ParameterizedMap parametized, Set<Class<?>> visited,
            Repetition repetition) {
        Class<?> keyType = parametized.getKeyActualType();
        Type nestedKey = buildTypeElement(keyType, visited, REQUIRED, "key");

        if (parametized.valueIsCollection()) {
            Type childCollection = createCollectionType("value", parametized.getValueTypeAsCollection(),
                    visited, OPTIONAL);
            return Types.map(repetition).key(nestedKey).value(childCollection).named(fieldName);
        }
        if (parametized.valueIsMap()) {
            Type childMap = createMapType("value", parametized.getValueTypeAsMap(), visited, OPTIONAL);
            return Types.map(repetition).key(nestedKey).value(childMap).named(fieldName);
        }

        Class<?> valueType = parametized.getValueActualType();
        Type nestedValue = buildTypeElement(valueType, visited, OPTIONAL, "value");
        if (nestedKey != null && nestedValue != null) {
            // TODO: what to change to support generation of older versions?
            return Types.map(repetition).key(nestedKey).value(nestedValue).named(fieldName);
        }
        throw new RecordTypeConversionException("Unsuported type in Map");
    }

    private Type buildTypeElement(Class<?> type, Set<Class<?>> visited, Repetition repetition, String name) {
        Type parquetType = buildType(type, visited, repetition, name);
        if (parquetType == null) {
            throw new RecordTypeConversionException("Unsuported type " + type);
        }
        return parquetType;
    }

    private Type buildType(Class<?> type, Set<Class<?>> visited, Repetition repetition, String name) {
        JavaType javaType = new JavaType(type);
        PrimitiveType primitiveType = simpleTypeItems(javaType, repetition, name);
        if (primitiveType != null) {
            return primitiveType;
        }
        if (javaType.isRecord()) {
            List<Type> childFields = buildCompositeChild(type, visited);
            return new GroupType(repetition, name, childFields);
        }
        if (javaType.isString()) {
            return primitive(BINARY, repetition).as(stringType()).named(name);
        }
        if (javaType.isEnum()) {
            return primitive(BINARY, repetition).as(enumType()).named(name);
        }
        if (javaType.isBinary()) {
            return primitive(BINARY, repetition).named(name);
        }
        if (javaType.isUuid()) {
            return primitive(FIXED_LEN_BYTE_ARRAY, repetition).as(uuidType())
                    .length(UUIDLogicalTypeAnnotation.BYTES)
                    .named(name);
        }
        if (javaType.isBigDecimal()) {
            return decimalTypeItem(repetition, name);
        }
        PrimitiveType dateTypeItems = dateTypeItems(javaType, repetition, name);
        if (dateTypeItems != null) {
            return dateTypeItems;
        }
        return null;
    }

    private PrimitiveType simpleTypeItems(JavaType javaType, Repetition repetition, String name) {
        if (javaType.isInteger()) {
            return primitive(PrimitiveTypeName.INT32, repetition).named(name);
        }
        if (javaType.isLong()) {
            return primitive(PrimitiveTypeName.INT64, repetition).named(name);
        }
        if (javaType.isFloat()) {
            return primitive(PrimitiveTypeName.FLOAT, repetition).named(name);
        }
        if (javaType.isDouble()) {
            return primitive(PrimitiveTypeName.DOUBLE, repetition).named(name);
        }
        if (javaType.isBoolean()) {
            return primitive(PrimitiveTypeName.BOOLEAN, repetition).named(name);
        }
        if (javaType.isShort()) {
            return primitive(PrimitiveTypeName.INT32, repetition).as(intType(16, true)).named(name);
        }
        if (javaType.isByte()) {
            return primitive(PrimitiveTypeName.INT32, repetition).as(intType(8, true)).named(name);
        }
        return null;
    }

    private PrimitiveType dateTypeItems(JavaType javaType, Repetition repetition, String name) {
        if (javaType.isLocalDate()) {
            return primitive(PrimitiveTypeName.INT32, repetition).as(dateType()).named(name);
        }
        if (javaType.isLocalTime()) {
            var timeUnit = carpetConfiguration.defaultTimeUnit();
            var timeType = timeType(carpetConfiguration.defaultTimeIsAdjustedToUTC(), toParquetTimeUnit(timeUnit));
            var typeName = switch (timeUnit) {
            case MILLIS -> PrimitiveTypeName.INT32;
            case MICROS, NANOS -> PrimitiveTypeName.INT64;
            };
            return primitive(typeName, repetition).as(timeType).named(name);
        }
        if (javaType.isLocalDateTime()) {
            var timeUnit = carpetConfiguration.defaultTimeUnit();
            var timeStampType = timestampType(false, toParquetTimeUnit(timeUnit));
            return primitive(PrimitiveTypeName.INT64, repetition).as(timeStampType).named(name);
        }
        if (javaType.isInstant()) {
            var timeUnit = carpetConfiguration.defaultTimeUnit();
            var timeStampType = timestampType(true, toParquetTimeUnit(timeUnit));
            return primitive(PrimitiveTypeName.INT64, repetition).as(timeStampType).named(name);
        }
        return null;
    }

    private static LogicalTypeAnnotation.TimeUnit toParquetTimeUnit(TimeUnit timeUnit) {
        return switch (timeUnit) {
        case MILLIS -> LogicalTypeAnnotation.TimeUnit.MILLIS;
        case MICROS -> LogicalTypeAnnotation.TimeUnit.MICROS;
        case NANOS -> LogicalTypeAnnotation.TimeUnit.NANOS;
        };
    }

    private Type decimalTypeItem(Repetition repetition, String name) {
        DecimalConfig decimalConfig = carpetConfiguration.decimalConfig();
        if (!decimalConfig.arePrecisionAndScaleConfigured()) {
            throw new RecordTypeConversionException("If BigDecimall is used, a Default Decimal configuration "
                    + "must be provided in the setup of CarpetWriter builder");
        }
        var decimalType = decimalType(decimalConfig.scale(), decimalConfig.precision());
        if (decimalConfig.precision() <= 9) {
            return primitive(PrimitiveTypeName.INT32, repetition).as(decimalType).named(name);
        }
        if (decimalConfig.precision() <= 18) {
            return primitive(PrimitiveTypeName.INT64, repetition).as(decimalType).named(name);
        }
        return primitive(PrimitiveTypeName.BINARY, repetition).as(decimalType).named(name);
    }

    private void validateNotVisitedRecord(Class<?> recordClass, Set<Class<?>> visited) {
        if (!recordClass.isRecord()) {
            throw new RecordTypeConversionException(recordClass.getName() + " must be a java Record");
        }
        if (visited.contains(recordClass)) {
            throw new RecordTypeConversionException("Recusive records are not supported");
        }
        visited.add(recordClass);
    }

}