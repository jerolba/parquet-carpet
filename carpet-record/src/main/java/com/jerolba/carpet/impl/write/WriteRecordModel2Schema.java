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
import static org.apache.parquet.schema.LogicalTypeAnnotation.variantType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.apache.parquet.schema.Types.buildGroup;
import static org.apache.parquet.schema.Types.primitive;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.parquet.column.schema.EdgeInterpolationAlgorithm;
import org.apache.parquet.schema.ConversionPatterns;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.model.BigDecimalType;
import com.jerolba.carpet.model.BinaryGeospatialType;
import com.jerolba.carpet.model.BinaryLogicalType;
import com.jerolba.carpet.model.CollectionType;
import com.jerolba.carpet.model.FieldType;
import com.jerolba.carpet.model.GeometryType;
import com.jerolba.carpet.model.GeometryType.GeospatialType;
import com.jerolba.carpet.model.MapType;
import com.jerolba.carpet.model.WriteRecordModelType;

class WriteRecordModel2Schema {

    private static final String KEY = "key";
    private static final String VALUE = "value";
    private static final String ELEMENT = "element";

    private final CarpetWriteConfiguration carpetConfiguration;

    public WriteRecordModel2Schema(CarpetWriteConfiguration carpetConfiguration) {
        this.carpetConfiguration = carpetConfiguration;
    }

    public MessageType createSchema(WriteRecordModelType<?> writeRecordModelType) {
        HashSet<WriteRecordModelType<?>> visited = new HashSet<>();
        validateNotVisitedRecord(writeRecordModelType, visited);

        String groupName = writeRecordModelType.getClassType().getSimpleName();
        List<Type> fields = createGroupFields(writeRecordModelType, visited);
        return new MessageType(groupName, fields);
    }

    private List<Type> createGroupFields(WriteRecordModelType<?> writeRecordModelType,
            Set<WriteRecordModelType<?>> visited) {

        List<Type> fields = new ArrayList<>();
        for (var recordField : writeRecordModelType.getFields()) {
            Repetition repetition = getTypeRepetition(recordField.fieldType());
            Type type = createType(recordField.fieldType(), recordField.parquetFieldName(), repetition, visited);
            if (type != null) {
                fields.add(type);
            } else {
                throw new RecordTypeConversionException(
                        "Column '" + recordField.parquetFieldName() + "' of type " +
                                recordField.fieldType().getClass().getName() + " not supported");
            }
        }
        return fields;
    }

    private Type createType(FieldType fieldtype, String parquetFieldName, Repetition repetition,
            Set<WriteRecordModelType<?>> visited) {
        if (fieldtype instanceof CollectionType collectionType) {
            return createCollectionType(parquetFieldName, collectionType.type(), repetition, visited);
        } else if (fieldtype instanceof MapType mapType) {
            return createMapType(parquetFieldName, mapType, repetition, visited);
        }
        return buildIfNonCollectionParquetType(fieldtype, parquetFieldName, repetition, visited);
    }

    private Type createCollectionType(String parquetFieldName, FieldType collectionType,
            Repetition repetition, Set<WriteRecordModelType<?>> visited) {
        return switch (carpetConfiguration.annotatedLevels()) {
        case ONE -> createCollectionOneLevel(parquetFieldName, collectionType, visited);
        case TWO -> createCollectionTwoLevel(parquetFieldName, collectionType, repetition, visited);
        case THREE -> createCollectionThreeLevel(parquetFieldName, collectionType, repetition, visited);
        };
    }

    private Type createCollectionOneLevel(String parquetFieldName, FieldType parametized,
            Set<WriteRecordModelType<?>> visited) {

        if (parametized instanceof CollectionType) {
            throw new RecordTypeConversionException(
                    "Recursive collections not supported in annotated 1-level structures");
        }
        if (parametized instanceof MapType mapType) {
            return createMapType(parquetFieldName, mapType, REPEATED, visited);
        }
        return buildTypeElement(parquetFieldName, parametized, REPEATED, visited);
    }

    private Type createCollectionTwoLevel(String parquetFieldName, FieldType parametized,
            Repetition repetition, Set<WriteRecordModelType<?>> visited) {
        // Two level collections elements are not nullables
        Type nested = createNestedCollection(parametized, REPEATED, visited);
        return ConversionPatterns.listType(repetition, parquetFieldName, nested);
    }

    private Type createCollectionThreeLevel(String parquetFieldName, FieldType parametized,
            Repetition repetition, Set<WriteRecordModelType<?>> visited) {
        Type nested = createNestedCollection(parametized, getTypeRepetition(parametized), visited);
        return ConversionPatterns.listOfElements(repetition, parquetFieldName, nested);
    }

    private Type createNestedCollection(FieldType parametized, Repetition repetition,
            Set<WriteRecordModelType<?>> visited) {
        if (parametized instanceof CollectionType collectionType) {
            return createCollectionType(ELEMENT, collectionType.type(), repetition, visited);
        }
        if (parametized instanceof MapType mapType) {
            return createMapType(ELEMENT, mapType, repetition, visited);
        }
        return buildTypeElement(ELEMENT, parametized, repetition, visited);
    }

    private Type createMapType(String parquetFieldName, MapType mapType, Repetition repetition,
            Set<WriteRecordModelType<?>> visited) {
        FieldType keyType = mapType.keyType();
        Type nestedKey = buildTypeElement(KEY, keyType, REQUIRED, visited);
        Type nestedValue = null;

        FieldType valueType = mapType.valueType();
        Repetition valueRepetition = getTypeRepetition(valueType);
        if (valueType instanceof CollectionType collectionType) {
            nestedValue = createCollectionType(VALUE, collectionType.type(), valueRepetition, visited);
        } else if (valueType instanceof MapType innerMapType) {
            nestedValue = createMapType(VALUE, innerMapType, valueRepetition, visited);
        } else {
            nestedValue = buildTypeElement(VALUE, valueType, valueRepetition, visited);
        }
        if (nestedKey != null && nestedValue != null) {
            // TODO: what to change to support generation of older versions?
            return Types.map(repetition).key(nestedKey).value(nestedValue).named(parquetFieldName);
        }
        throw new RecordTypeConversionException("Unsuported type in Map");
    }

    private Type buildTypeElement(String name, FieldType type, Repetition repetition,
            Set<WriteRecordModelType<?>> visited) {
        Type parquetType = buildIfNonCollectionParquetType(type, name, repetition, visited);
        if (parquetType == null) {
            throw new RecordTypeConversionException("Unsuported type " + type);
        }
        return parquetType;
    }

    private Type buildIfNonCollectionParquetType(FieldType type, String parquetFieldName, Repetition repetition,
            Set<WriteRecordModelType<?>> visited) {

        FieldTypeInspect javaType = new FieldTypeInspect(type);
        if (javaType.isInteger()) {
            return primitive(PrimitiveTypeName.INT32, repetition).named(parquetFieldName);
        } else if (javaType.isLong()) {
            return primitive(PrimitiveTypeName.INT64, repetition).named(parquetFieldName);
        } else if (javaType.isFloat()) {
            return primitive(PrimitiveTypeName.FLOAT, repetition).named(parquetFieldName);
        } else if (javaType.isDouble()) {
            return primitive(PrimitiveTypeName.DOUBLE, repetition).named(parquetFieldName);
        } else if (javaType.isBoolean()) {
            return primitive(PrimitiveTypeName.BOOLEAN, repetition).named(parquetFieldName);
        } else if (javaType.isShort()) {
            return primitive(PrimitiveTypeName.INT32, repetition).as(intType(16, true)).named(parquetFieldName);
        } else if (javaType.isByte()) {
            return primitive(PrimitiveTypeName.INT32, repetition).as(intType(8, true)).named(parquetFieldName);
        } else if (javaType.isString()) {
            return buildStringType(javaType.binaryLogicalType(), repetition, parquetFieldName);
        } else if (javaType.isJtsGeometry()) {
            GeometryType geometryType = javaType.geometryType();
            return buildGeospatialType(repetition, parquetFieldName,
                    geometryType.geospatialType(), geometryType.crs(), geometryType.algorithm());
        } else if (javaType.isBinaryGeospatial()) {
            BinaryGeospatialType geospatial = javaType.binaryGeospatialType();
            return buildGeospatialType(repetition, parquetFieldName,
                    geospatial.geospatialType(), geospatial.crs(), geospatial.algorithm());
        } else if (javaType.isBinary()) {
            return buildBinaryType(javaType.binaryLogicalType(), repetition, parquetFieldName);
        } else if (javaType.isEnum()) {
            return buildEnumType(javaType.binaryLogicalType(), repetition, parquetFieldName);
        } else if (javaType.isUuid()) {
            return buildUuidType(repetition, parquetFieldName);
        } else if (javaType.isBigDecimal()) {
            var bigDecimalType = (BigDecimalType) type;
            var config = buildDecimalConfig(bigDecimalType.precision(), bigDecimalType.scale(),
                    bigDecimalType.roundingMode(), carpetConfiguration.decimalConfig());
            return buildDecimalTypeItem(repetition, parquetFieldName, config);
        } else if (javaType.isLocalDate()) {
            return buildLocalDateType(repetition, parquetFieldName);
        } else if (javaType.isLocalTime()) {
            return buildLocalTimeType(repetition, parquetFieldName,
                    carpetConfiguration.defaultTimeUnit(), carpetConfiguration.defaultTimeIsAdjustedToUTC());
        } else if (javaType.isLocalDateTime()) {
            return buildLocalDateTimeType(repetition, parquetFieldName, carpetConfiguration.defaultTimeUnit());
        } else if (javaType.isInstant()) {
            return buildInstantType(repetition, parquetFieldName, carpetConfiguration.defaultTimeUnit());
        } else if (javaType.isVariant()) {
            return buildUnshreddedVariantType(repetition, parquetFieldName);
        } else if (type instanceof WriteRecordModelType<?> childWriteRecordType) {
            List<Type> childFields = buildChildFields(childWriteRecordType, visited);
            return new GroupType(repetition, parquetFieldName, childFields);
        }
        return null;
    }

    private Type buildStringType(BinaryLogicalType logicalType, Repetition repetition, String parquetFieldName) {
        var binary = primitive(BINARY, repetition);
        if (logicalType == null) {
            return binary.as(stringType()).named(parquetFieldName);
        }
        return switch (logicalType) {
        case JSON -> binary.as(jsonType()).named(parquetFieldName);
        case ENUM -> binary.as(enumType()).named(parquetFieldName);
        case STRING -> binary.as(stringType()).named(parquetFieldName);
        case BSON -> throw new RecordTypeConversionException("Unsupported logical type for String: " + logicalType);
        };
    }

    private Type buildBinaryType(BinaryLogicalType logicalType, Repetition repetition, String parquetFieldName) {
        var binary = primitive(BINARY, repetition);
        if (logicalType == null) {
            return binary.named(parquetFieldName);
        }
        return switch (logicalType) {
        case STRING -> binary.as(stringType()).named(parquetFieldName);
        case ENUM -> binary.as(enumType()).named(parquetFieldName);
        case JSON -> binary.as(jsonType()).named(parquetFieldName);
        case BSON -> binary.as(bsonType()).named(parquetFieldName);
        };
    }

    private Type buildGeospatialType(Repetition repetition, String parquetFieldName, GeospatialType geospatialType,
            String crs, EdgeInterpolationAlgorithm algorithm) {
        return switch (geospatialType) {
        case GEOMETRY -> primitive(BINARY, repetition).as(geometryType(crs)).named(parquetFieldName);
        case GEOGRAPHY -> primitive(BINARY, repetition).as(geographyType(crs, algorithm)).named(parquetFieldName);
        };
    }

    private Type buildEnumType(BinaryLogicalType logicalType, Repetition repetition, String parquetFieldName) {
        var binary = primitive(BINARY, repetition);
        if (logicalType == null) {
            return binary.as(enumType()).named(parquetFieldName);
        }
        return switch (logicalType) {
        case STRING -> binary.as(stringType()).named(parquetFieldName);
        case ENUM -> binary.as(enumType()).named(parquetFieldName);
        case BSON, JSON -> throw new RecordTypeConversionException(
                "Unsupported logical type for String: " + logicalType);
        };
    }

    private Type buildUnshreddedVariantType(Repetition repetition, String parquetFieldName) {
        return buildGroup(repetition)
                .as(variantType((byte) 1))
                .addField(primitive(BINARY, REQUIRED).named("metadata"))
                .addField(primitive(BINARY, REQUIRED).named("value"))
                .named(parquetFieldName);
    }

    private List<Type> buildChildFields(WriteRecordModelType<?> writeRecordType, Set<WriteRecordModelType<?>> visited) {
        validateNotVisitedRecord(writeRecordType, visited);
        List<Type> fields = createGroupFields(writeRecordType, visited);
        visited.remove(writeRecordType);
        return fields;
    }

    private void validateNotVisitedRecord(WriteRecordModelType<?> recordClass, Set<WriteRecordModelType<?>> visited) {
        if (visited.contains(recordClass)) {
            throw new RecordTypeConversionException("Recusive records are not supported");
        }
        visited.add(recordClass);
    }

    private static Repetition getTypeRepetition(FieldType parametized) {
        return parametized.isNotNull() ? REQUIRED : OPTIONAL;
    }
}