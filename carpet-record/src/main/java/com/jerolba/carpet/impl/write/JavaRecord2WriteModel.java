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
import static com.jerolba.carpet.model.FieldTypes.LIST;
import static com.jerolba.carpet.model.FieldTypes.MAP;
import static com.jerolba.carpet.model.FieldTypes.writeRecordModel;

import java.lang.reflect.TypeVariable;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.UnaryOperator;

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
import com.jerolba.carpet.impl.Parameterized;
import com.jerolba.carpet.impl.ParameterizedCollection;
import com.jerolba.carpet.impl.ParameterizedMap;
import com.jerolba.carpet.model.BigDecimalType;
import com.jerolba.carpet.model.BinaryType;
import com.jerolba.carpet.model.BooleanType;
import com.jerolba.carpet.model.ByteType;
import com.jerolba.carpet.model.DoubleType;
import com.jerolba.carpet.model.EnumType;
import com.jerolba.carpet.model.FieldType;
import com.jerolba.carpet.model.FieldTypes;
import com.jerolba.carpet.model.FloatType;
import com.jerolba.carpet.model.GeometryType;
import com.jerolba.carpet.model.InstantType;
import com.jerolba.carpet.model.IntegerType;
import com.jerolba.carpet.model.ListType;
import com.jerolba.carpet.model.LocalDateTimeType;
import com.jerolba.carpet.model.LocalDateType;
import com.jerolba.carpet.model.LocalTimeType;
import com.jerolba.carpet.model.LongType;
import com.jerolba.carpet.model.MapType;
import com.jerolba.carpet.model.ShortType;
import com.jerolba.carpet.model.StringType;
import com.jerolba.carpet.model.UuidType;
import com.jerolba.carpet.model.VariantType;
import com.jerolba.carpet.model.WriteRecordModelType;

public class JavaRecord2WriteModel {

    private final FieldToColumnMapper fieldToColumnMapper;

    public JavaRecord2WriteModel(CarpetWriteConfiguration carpetConfiguration) {
        this.fieldToColumnMapper = new FieldToColumnMapper(carpetConfiguration.columnNamingStrategy());
    }

    public <T> WriteRecordModelType<T> createModel(Class<T> recordClass) {
        return buildRecordModel(recordClass, new Modifiers(false), new HashSet<>());
    }

    private <T> WriteRecordModelType<T> buildRecordModel(Class<T> classToModel, Modifiers modifiers,
            Set<Class<?>> visited) {
        visited = validateNotVisitedRecord(classToModel, visited);

        WriteRecordModelType<T> writeModel = writeRecordModel(classToModel);
        createRecordFields(writeModel, classToModel, visited);
        return modifiers.modify(writeModel, WriteRecordModelType::notNull, WriteRecordModelType::fieldId);
    }

    private <T> void createRecordFields(WriteRecordModelType<T> writeModel, Class<T> recordClass,
            Set<Class<?>> visited) {
        FieldIdMapper fieldIdMapper = new FieldIdMapper();
        for (var attr : recordClass.getRecordComponents()) {
            java.lang.reflect.Type genericType = attr.getGenericType();
            if (genericType instanceof TypeVariable<?>) {
                throw new RecordTypeConversionException(genericType.toString() + " generic types not supported");
            }
            String parquetFieldName = fieldToColumnMapper.getColumnName(attr);
            Class<?> type = attr.getType();
            var javaType = new JavaType(type, attr.getDeclaredAnnotations());
            boolean notNull = type.isPrimitive() || isNotNull(attr);
            var modifiers = new Modifiers(notNull, fieldIdMapper.getFieldId(attr));
            FieldType fieldType = null;
            if (javaType.isCollection()) {
                var parameterizedCollection = Parameterized.getParameterizedCollection(attr);
                fieldType = createCollectionType(parameterizedCollection, modifiers, visited);
            } else if (javaType.isMap()) {
                var parameterizedMap = Parameterized.getParameterizedMap(attr);
                fieldType = createMapType(parameterizedMap, modifiers, visited);
            } else {
                fieldType = simpleOrCompositeClass(javaType, modifiers, visited);
            }
            if (!type.isPrimitive()) {
                writeModel.withField(parquetFieldName, fieldType,
                        (Function<T, Object>) Reflection.recordAccessor(recordClass, attr));
            } else if (javaType.isInteger()) {
                writeModel.withPrimitiveField(parquetFieldName, fieldType,
                        Reflection.intFieldAccessor(recordClass, attr.getName()));
            } else if (javaType.isLong()) {
                writeModel.withPrimitiveField(parquetFieldName, fieldType,
                        Reflection.longFieldAccessor(recordClass, attr.getName()));
            } else if (javaType.isFloat()) {
                writeModel.withPrimitiveField(parquetFieldName, fieldType,
                        Reflection.floatFieldAccessor(recordClass, attr.getName()));
            } else if (javaType.isDouble()) {
                writeModel.withPrimitiveField(parquetFieldName, fieldType,
                        Reflection.doubleFieldAccessor(recordClass, attr.getName()));
            } else if (javaType.isShort()) {
                writeModel.withPrimitiveField(parquetFieldName, fieldType,
                        Reflection.shortFieldAccessor(recordClass, attr.getName()));
            } else if (javaType.isByte()) {
                writeModel.withPrimitiveField(parquetFieldName, fieldType,
                        Reflection.byteFieldAccessor(recordClass, attr.getName()));
            } else if (javaType.isBoolean()) {
                writeModel.withPrimitiveField(parquetFieldName, fieldType,
                        Reflection.booleanFieldAccessor(recordClass, attr.getName()));
            } else {
                throw new RecordTypeConversionException("Unsupported primitive type: " + type);
            }
        }
    }

    private FieldType simpleOrCompositeClass(JavaType javaType, Modifiers modifiers, Set<Class<?>> visited) {
        FieldType simple = buildSimpleType(javaType, modifiers);
        return simple == null ? buildRecordModel(javaType.getJavaType(), modifiers, visited) : simple;
    }

    private ListType createCollectionType(ParameterizedCollection generic, Modifiers modifiers, Set<Class<?>> visited) {
        ListType list = LIST.ofType(createGenericType(generic, visited));
        return modifiers.modify(list, ListType::notNull, ListType::fieldId);
    }

    private FieldType createGenericType(ParameterizedCollection generic, Set<Class<?>> visited) {
        JavaType actualJavaType = generic.getActualJavaType();
        boolean typeIsNotNull = isNotNullAnnotated(actualJavaType.getDeclaredAnnotations());
        var modifiers = new Modifiers(typeIsNotNull);
        if (generic.isCollection()) {
            return createCollectionType(generic.getAsCollection(), modifiers, visited);
        } else if (generic.isMap()) {
            return createMapType(generic.getAsMap(), modifiers, visited);
        }
        return simpleOrCompositeClass(actualJavaType, modifiers, visited);
    }

    private MapType createMapType(ParameterizedMap parametized, Modifiers modifiers, Set<Class<?>> visited) {
        ParameterizedCollection genericKey = parametized.getGenericKey();
        if (genericKey.isCollection() || genericKey.isMap()) {
            throw new RuntimeException("Maps with collections or maps as keys are not supported");
        }

        FieldType nestedKey = simpleOrCompositeClass(genericKey.getActualJavaType(), new Modifiers(true), visited);
        FieldType nestedValue = createGenericType(parametized.getGenericValue(), visited);
        if (nestedKey != null && nestedValue != null) {
            MapType map = MAP.ofTypes(nestedKey, nestedValue);
            return modifiers.modify(map, MapType::notNull, MapType::fieldId);
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

    public static FieldType buildSimpleType(JavaType javaType, Modifiers modifiers) {
        if (javaType.isInteger()) {
            return modifiers.modify(FieldTypes.INTEGER, IntegerType::notNull, IntegerType::fieldId);
        }
        if (javaType.isLong()) {
            return modifiers.modify(FieldTypes.LONG, LongType::notNull, LongType::fieldId);
        }
        if (javaType.isFloat()) {
            return modifiers.modify(FieldTypes.FLOAT, FloatType::notNull, FloatType::fieldId);
        }
        if (javaType.isDouble()) {
            return modifiers.modify(FieldTypes.DOUBLE, DoubleType::notNull, DoubleType::fieldId);
        }
        if (javaType.isBoolean()) {
            return modifiers.modify(FieldTypes.BOOLEAN, BooleanType::notNull, BooleanType::fieldId);
        }
        if (javaType.isShort()) {
            return modifiers.modify(FieldTypes.SHORT, ShortType::notNull, ShortType::fieldId);
        }
        if (javaType.isByte()) {
            return modifiers.modify(FieldTypes.BYTE, ByteType::notNull, ByteType::fieldId);
        }
        if (javaType.isString()) {
            return modifiers.modify(stringType(javaType), StringType::notNull, StringType::fieldId);
        }
        if (javaType.isBinary()) {
            return modifiers.modify(binaryType(javaType), BinaryType::notNull, BinaryType::fieldId);
        }
        if (javaType.isEnum()) {
            return modifiers.modify(enumType(javaType), EnumType::notNull, EnumType::fieldId);
        }
        if (javaType.isUuid()) {
            return modifiers.modify(FieldTypes.UUID, UuidType::notNull, UuidType::fieldId);
        }
        if (javaType.isBigDecimal()) {
            return modifiers.modify(bigDecimalType(javaType), BigDecimalType::notNull, BigDecimalType::fieldId);
        }
        if (javaType.isLocalDate()) {
            return modifiers.modify(FieldTypes.LOCAL_DATE, LocalDateType::notNull, LocalDateType::fieldId);
        }
        if (javaType.isLocalTime()) {
            return modifiers.modify(FieldTypes.LOCAL_TIME, LocalTimeType::notNull, LocalTimeType::fieldId);
        }
        if (javaType.isLocalDateTime()) {
            return modifiers.modify(FieldTypes.LOCAL_DATE_TIME, LocalDateTimeType::notNull, LocalDateTimeType::fieldId);
        }
        if (javaType.isInstant()) {
            return modifiers.modify(FieldTypes.INSTANT, InstantType::notNull, InstantType::fieldId);
        }
        if (javaType.isGeometry()) {
            return modifiers.modify(geometryType(javaType), GeometryType::notNull, GeometryType::fieldId);
        }
        if (javaType.isVariant()) {
            return modifiers.modify(FieldTypes.VARIANT, VariantType::notNull, VariantType::fieldId);
        }
        return null;
    }

    private static StringType stringType(JavaType javaType) {
        StringType type = FieldTypes.STRING;
        if (javaType.isAnnotatedWith(ParquetJson.class)) {
            type = FieldTypes.STRING.asJson();
        } else if (javaType.isAnnotatedWith(ParquetEnum.class)) {
            type = FieldTypes.STRING.asEnum();
        }
        return type;
    }

    private static BinaryType binaryType(JavaType javaType) {
        BinaryType binary = FieldTypes.BINARY;
        if (javaType.isAnnotatedWith(ParquetString.class)) {
            return binary.asString();
        } else if (javaType.isAnnotatedWith(ParquetJson.class)) {
            return binary.asJson();
        } else if (javaType.isAnnotatedWith(ParquetEnum.class)) {
            return binary.asEnum();
        } else if (javaType.isAnnotatedWith(ParquetBson.class)) {
            return binary.asBson();
        } else if (javaType.isAnnotatedWith(ParquetGeometry.class)) {
            ParquetGeometry geometry = javaType.getAnnotation(ParquetGeometry.class);
            String csr = geometry.value();
            return binary.asParquetGeometry(csr == null || csr.isEmpty() ? null : csr);
        } else if (javaType.isAnnotatedWith(ParquetGeography.class)) {
            ParquetGeography geography = javaType.getAnnotation(ParquetGeography.class);
            return binary.asParquetGeography(geography.crs(), geography.algorithm().getAlgorithm());
        }
        return binary;
    }

    private static GeometryType geometryType(JavaType javaType) {
        if (javaType.isAnnotatedWith(ParquetGeometry.class)) {
            ParquetGeometry geometry = javaType.getAnnotation(ParquetGeometry.class);
            String csr = geometry.value();
            return FieldTypes.GEOMETRY.asParquetGeometry(csr == null || csr.isEmpty() ? null : csr);
        } else if (javaType.isAnnotatedWith(ParquetGeography.class)) {
            ParquetGeography geography = javaType.getAnnotation(ParquetGeography.class);
            String csr = geography.crs();
            return FieldTypes.GEOMETRY.asParquetGeography(csr == null || csr.isEmpty() ? null : csr,
                    geography.algorithm().getAlgorithm());
        }
        throw new RecordTypeConversionException("Geometry or Geography annotation is required for Geometry types");
    }

    private static EnumType enumType(JavaType javaType) {
        EnumType enumType = FieldTypes.ENUM.ofType((Class<? extends Enum<?>>) javaType.getJavaType());
        if (javaType.isAnnotatedWith(ParquetString.class)) {
            enumType = enumType.asString();
        }
        return enumType;
    }

    private static BigDecimalType bigDecimalType(JavaType javaType) {
        var bigDecimal = FieldTypes.BIG_DECIMAL;
        PrecisionScale precisionScale = javaType.getAnnotation(PrecisionScale.class);
        if (precisionScale != null) {
            bigDecimal = bigDecimal.withPrecisionScale(precisionScale.precision(), precisionScale.scale());
        }
        Rounding rounding = javaType.getAnnotation(Rounding.class);
        if (rounding != null) {
            bigDecimal = bigDecimal.withRoundingMode(rounding.value());
        }
        return bigDecimal;
    }

    private record Modifiers(boolean isNotNull, Integer fieldId) {

        Modifiers(boolean isNotNull) {
            this(isNotNull, null);
        }

        <T extends FieldType> T modify(T value, UnaryOperator<T> toNotNull, BiFunction<T, Integer, T> withFieldId) {
            T app = isNotNull ? toNotNull.apply(value) : value;
            if (fieldId != null) {
                app = withFieldId.apply(app, fieldId);
            }
            return app;
        }

    }
}