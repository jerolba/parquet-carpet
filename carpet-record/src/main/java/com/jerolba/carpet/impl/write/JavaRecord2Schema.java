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
import static com.jerolba.carpet.impl.NotNullField.isNotNullAnnotated;
import static com.jerolba.carpet.impl.Parameterized.getParameterizedCollection;
import static com.jerolba.carpet.impl.Parameterized.getParameterizedMap;
import static com.jerolba.carpet.impl.write.BigDecimalWrite.buildDecimalConfig;
import static com.jerolba.carpet.impl.write.SchemaBuilder.buildDecimalTypeItem;
import static com.jerolba.carpet.impl.write.SchemaBuilder.buildInstantType;
import static com.jerolba.carpet.impl.write.SchemaBuilder.buildLocalDateTimeType;
import static com.jerolba.carpet.impl.write.SchemaBuilder.buildLocalDateType;
import static com.jerolba.carpet.impl.write.SchemaBuilder.buildLocalTimeType;
import static com.jerolba.carpet.impl.write.SchemaBuilder.buildUuidType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.bsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.geographyType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.geometryType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.jsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.apache.parquet.schema.Types.primitive;

import java.lang.reflect.RecordComponent;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.parquet.schema.ConversionPatterns;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.PrimitiveBuilder;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.annotation.ParquetBson;
import com.jerolba.carpet.annotation.ParquetEnum;
import com.jerolba.carpet.annotation.ParquetGeography;
import com.jerolba.carpet.annotation.ParquetGeometry;
import com.jerolba.carpet.annotation.ParquetJson;
import com.jerolba.carpet.annotation.ParquetString;
import com.jerolba.carpet.annotation.PrecisionScale;
import com.jerolba.carpet.annotation.Rounding;
import com.jerolba.carpet.impl.JavaType;
import com.jerolba.carpet.impl.ParameterizedCollection;
import com.jerolba.carpet.impl.ParameterizedMap;

class JavaRecord2Schema {

    private static final String KEY = "key";
    private static final String VALUE = "value";
    private static final String ELEMENT = "element";

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
            fields.add(buildRecordField(attr, visited));
        }
        return fields;
    }

    private Type buildRecordField(RecordComponent attr, Set<Class<?>> visited) {
        String fieldName = fieldToColumnMapper.getColumnName(attr);
        Repetition repetition = isNotNull(attr) ? REQUIRED : OPTIONAL;
        JavaType javaType = new JavaType(attr);

        Type parquetType = buildType(fieldName, javaType, repetition, visited);
        if (parquetType != null) {
            return parquetType;
        } else if (javaType.isCollection()) {
            return createCollectionType(fieldName, getParameterizedCollection(attr), repetition, visited);
        } else if (javaType.isMap()) {
            return createMapType(fieldName, getParameterizedMap(attr), repetition, visited);
        }
        if (attr.getGenericType() instanceof TypeVariable<?>) {
            throw new RecordTypeConversionException(attr.getGenericType().toString() + " generic types not supported");
        }
        throw new RecordTypeConversionException(
                "Field '" + attr.getName() + "' of type " + attr.getType() + " not supported");
    }

    private List<Type> buildCompositeChild(Class<?> recordClass, Set<Class<?>> visited) {
        validateNotVisitedRecord(recordClass, visited);
        List<Type> fields = createGroupFields(recordClass, visited);
        visited.remove(recordClass);
        return fields;
    }

    private Type createCollectionType(String fieldName, ParameterizedCollection collectionClass, Repetition repetition,
            Set<Class<?>> visited) {
        return switch (carpetConfiguration.annotatedLevels()) {
        case ONE -> createCollectionOneLevel(fieldName, collectionClass, visited);
        case TWO -> createCollectionTwoLevel(fieldName, collectionClass, repetition, visited);
        case THREE -> createCollectionThreeLevel(fieldName, collectionClass, repetition, visited);
        };
    }

    private Type createCollectionOneLevel(String fieldName, ParameterizedCollection parametized,
            Set<Class<?>> visited) {
        if (parametized.isCollection()) {
            throw new RecordTypeConversionException(
                    "Recursive collections not supported in annotated 1-level structures");
        }
        if (parametized.isMap()) {
            return createMapType(fieldName, parametized.getParametizedAsMap(), REPEATED, visited);
        }
        return buildTypeElement(fieldName, parametized.getActualJavaType(), REPEATED, visited);
    }

    private Type createCollectionTwoLevel(String fieldName, ParameterizedCollection parametized, Repetition repetition,
            Set<Class<?>> visited) {
        // Two level collections elements are not nullables
        Type nested = createNestedCollection(parametized, REPEATED, visited);
        return ConversionPatterns.listType(repetition, fieldName, nested);
    }

    private Type createCollectionThreeLevel(String fieldName, ParameterizedCollection parametized,
            Repetition repetition, Set<Class<?>> visited) {
        var repetitionCollection = getTypeRepetition(parametized.getActualJavaType());
        Type nested = createNestedCollection(parametized, repetitionCollection, visited);
        return ConversionPatterns.listOfElements(repetition, fieldName, nested);
    }

    private Type createNestedCollection(ParameterizedCollection parametized, Repetition repetition,
            Set<Class<?>> visited) {
        if (parametized.isCollection()) {
            return createCollectionType(ELEMENT, parametized.getParametizedAsCollection(), repetition, visited);
        }
        if (parametized.isMap()) {
            return createMapType(ELEMENT, parametized.getParametizedAsMap(), repetition, visited);
        }
        return buildTypeElement(ELEMENT, parametized.getActualJavaType(), repetition, visited);
    }

    private Type createMapType(String fieldName, ParameterizedMap parametized, Repetition repetition,
            Set<Class<?>> visited) {
        Type nestedKey = buildTypeElement(KEY, parametized.getKeyActualJavaType(), REQUIRED, visited);
        Type nestedValue = null;

        var repetitionValue = getTypeRepetition(parametized.getValueActualJavaType());
        if (parametized.valueIsCollection()) {
            nestedValue = createCollectionType(VALUE, parametized.getValueTypeAsCollection(), repetitionValue,
                    visited);
        } else if (parametized.valueIsMap()) {
            nestedValue = createMapType(VALUE, parametized.getValueTypeAsMap(), repetitionValue, visited);
        } else {
            nestedValue = buildTypeElement(VALUE, parametized.getValueActualJavaType(), repetitionValue, visited);
        }
        if (nestedKey != null && nestedValue != null) {
            // TODO: what to change to support generation of older versions?
            return Types.map(repetition).key(nestedKey).value(nestedValue).named(fieldName);
        }
        throw new RecordTypeConversionException("Unsupported type in Map");
    }

    private Type buildTypeElement(String name, JavaType javaType, Repetition repetition, Set<Class<?>> visited) {
        Type parquetType = buildType(name, javaType, repetition, visited);
        if (parquetType == null) {
            throw new RecordTypeConversionException("Unsupported type " + javaType.getJavaType());
        }
        return parquetType;
    }

    private Type buildType(String name, JavaType javaType, Repetition repetition, Set<Class<?>> visited) {
        if (javaType.isInteger()) {
            return primitive(PrimitiveTypeName.INT32, repetition).named(name);
        } else if (javaType.isLong()) {
            return primitive(PrimitiveTypeName.INT64, repetition).named(name);
        } else if (javaType.isFloat()) {
            return primitive(PrimitiveTypeName.FLOAT, repetition).named(name);
        } else if (javaType.isDouble()) {
            return primitive(PrimitiveTypeName.DOUBLE, repetition).named(name);
        } else if (javaType.isBoolean()) {
            return primitive(PrimitiveTypeName.BOOLEAN, repetition).named(name);
        } else if (javaType.isShort()) {
            return primitive(PrimitiveTypeName.INT32, repetition).as(intType(16, true)).named(name);
        } else if (javaType.isByte()) {
            return primitive(PrimitiveTypeName.INT32, repetition).as(intType(8, true)).named(name);
        } else if (javaType.isString()) {
            return buildStringType(javaType, repetition, name);
        } else if (javaType.isBinary()) {
            return buildBinaryType(javaType, repetition, name);
        } else if (javaType.isEnum()) {
            return buildEnumType(javaType, repetition, name);
        } else if (javaType.isUuid()) {
            return buildUuidType(repetition, name);
        } else if (javaType.isBigDecimal()) {
            DecimalConfig decimalConfig = buildDecimalConfig(javaType.getAnnotation(PrecisionScale.class),
                    javaType.getAnnotation(Rounding.class),
                    carpetConfiguration.decimalConfig());
            return buildDecimalTypeItem(repetition, name, decimalConfig);
        } else if (javaType.isLocalDate()) {
            return buildLocalDateType(repetition, name);
        } else if (javaType.isLocalTime()) {
            return buildLocalTimeType(repetition, name,
                    carpetConfiguration.defaultTimeUnit(), carpetConfiguration.defaultTimeIsAdjustedToUTC());
        } else if (javaType.isLocalDateTime()) {
            return buildLocalDateTimeType(repetition, name, carpetConfiguration.defaultTimeUnit());
        } else if (javaType.isInstant()) {
            return buildInstantType(repetition, name, carpetConfiguration.defaultTimeUnit());
        } else if (javaType.isRecord()) {
            List<Type> childFields = buildCompositeChild(javaType.getJavaType(), visited);
            return new GroupType(repetition, name, childFields);
        }
        return null;
    }

    private Type buildStringType(JavaType javaType, Repetition repetition, String name) {
        if (javaType.isAnnotatedWith(ParquetJson.class)) {
            return primitive(BINARY, repetition).as(jsonType()).named(name);
        } else if (javaType.isAnnotatedWith(ParquetEnum.class)) {
            return primitive(BINARY, repetition).as(enumType()).named(name);
        }
        return primitive(BINARY, repetition).as(stringType()).named(name);
    }

    private Type buildBinaryType(JavaType javaType, Repetition repetition, String name) {
        PrimitiveBuilder<PrimitiveType> binary = primitive(BINARY, repetition);
        if (javaType.isAnnotatedWith(ParquetString.class)) {
            binary = binary.as(stringType());
        } else if (javaType.isAnnotatedWith(ParquetEnum.class)) {
            binary = binary.as(enumType());
        } else if (javaType.isAnnotatedWith(ParquetJson.class)) {
            binary = binary.as(jsonType());
        } else if (javaType.isAnnotatedWith(ParquetBson.class)) {
            binary = binary.as(bsonType());
        } else if (javaType.isAnnotatedWith(ParquetGeometry.class)) {
            ParquetGeometry geometry = javaType.getAnnotation(ParquetGeometry.class);
            String csr = geometry.value();
            binary = binary.as(geometryType(csr == null || csr.isEmpty() ? null : csr));
        } else if (javaType.isAnnotatedWith(ParquetGeography.class)) {
            ParquetGeography geography = javaType.getAnnotation(ParquetGeography.class);
            binary = binary.as(geographyType(geography.crs(), geography.algorithm().getAlgorithm()));
        }
        return binary.named(name);
    }

    private Type buildEnumType(JavaType javaType, Repetition repetition, String name) {
        if (javaType.isAnnotatedWith(ParquetString.class)) {
            return primitive(BINARY, repetition).as(stringType()).named(name);
        }
        return primitive(BINARY, repetition).as(enumType()).named(name);
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

    private static Repetition getTypeRepetition(JavaType javaType) {
        return isNotNullAnnotated(javaType.getDeclaredAnnotations()) ? REQUIRED : OPTIONAL;
    }

}