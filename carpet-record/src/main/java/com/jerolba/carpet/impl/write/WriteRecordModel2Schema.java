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

import static org.apache.parquet.schema.LogicalTypeAnnotation.bsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.jsonType;
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
import com.jerolba.carpet.model.CollectionType;
import com.jerolba.carpet.model.FieldType;
import com.jerolba.carpet.model.MapType;
import com.jerolba.carpet.model.StringType.StringLogicalType;
import com.jerolba.carpet.model.WriteRecordModelType;

class WriteRecordModel2Schema {

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
            Repetition repetition = recordField.fieldType().isNotNull() ? REQUIRED : OPTIONAL;

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
        return buildTypeElement(parametized, parquetFieldName, REPEATED, visited);
    }

    private Type createCollectionTwoLevel(String parquetFieldName, FieldType parametized,
            Repetition repetition, Set<WriteRecordModelType<?>> visited) {
        // Two level collections elements are not nullables
        Type nested = createNestedCollection(parametized, REPEATED, visited);
        return ConversionPatterns.listType(repetition, parquetFieldName, nested);
    }

    private Type createCollectionThreeLevel(String parquetFieldName, FieldType parametized,
            Repetition repetition, Set<WriteRecordModelType<?>> visited) {
        // Three level collections elements are nullables
        Type nested = createNestedCollection(parametized, OPTIONAL, visited);
        return ConversionPatterns.listOfElements(repetition, parquetFieldName, nested);
    }

    private Type createNestedCollection(FieldType parametized, Repetition repetition,
            Set<WriteRecordModelType<?>> visited) {
        if (parametized instanceof CollectionType collectionType) {
            return createCollectionType("element", collectionType.type(), repetition, visited);
        }
        if (parametized instanceof MapType mapType) {
            return createMapType("element", mapType, repetition, visited);
        }
        return buildTypeElement(parametized, "element", repetition, visited);
    }

    private Type createMapType(String parquetFieldName, MapType mapType, Repetition repetition,
            Set<WriteRecordModelType<?>> visited) {
        FieldType keyType = mapType.keyType();
        Type nestedKey = buildTypeElement(keyType, "key", REQUIRED, visited);

        FieldType valueType = mapType.valueType();
        if (valueType instanceof CollectionType collectionType) {
            Type childCollection = createCollectionType("value", collectionType.type(), OPTIONAL, visited);
            return Types.map(repetition).key(nestedKey).value(childCollection).named(parquetFieldName);
        }
        if (valueType instanceof MapType innerMapType) {
            Type childMap = createMapType("value", innerMapType, OPTIONAL, visited);
            return Types.map(repetition).key(nestedKey).value(childMap).named(parquetFieldName);
        }

        Type nestedValue = buildTypeElement(valueType, "value", OPTIONAL, visited);
        if (nestedKey != null && nestedValue != null) {
            // TODO: what to change to support generation of older versions?
            return Types.map(repetition).key(nestedKey).value(nestedValue).named(parquetFieldName);
        }
        throw new RecordTypeConversionException("Unsuported type in Map");
    }

    private Type buildTypeElement(FieldType type, String name, Repetition repetition,
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
        PrimitiveType primitiveType = buildIfPrimitiveType(javaType, repetition, parquetFieldName);
        if (primitiveType != null) {
            return primitiveType;
        }
        if (javaType.isString()) {
            if (javaType.stringLogicalType() == StringLogicalType.JSON) {
                return primitive(BINARY, repetition).as(jsonType()).named(parquetFieldName);
            }
            return primitive(BINARY, repetition).as(stringType()).named(parquetFieldName);
        }
        if (javaType.isBinary()) {
            return switch (javaType.binaryLogicalType()) {
            case STRING -> primitive(BINARY, repetition).as(stringType()).named(parquetFieldName);
            case JSON -> primitive(BINARY, repetition).as(jsonType()).named(parquetFieldName);
            case BSON -> primitive(BINARY, repetition).as(bsonType()).named(parquetFieldName);
            default -> null;
            };
        }
        if (javaType.isEnum()) {
            return primitive(BINARY, repetition).as(enumType()).named(parquetFieldName);
        }
        if (javaType.isUuid()) {
            return primitive(FIXED_LEN_BYTE_ARRAY, repetition).as(uuidType())
                    .length(UUIDLogicalTypeAnnotation.BYTES).named(parquetFieldName);
        }
        if (javaType.isBigDecimal()) {
            return buildDecimalTypeItem(repetition, parquetFieldName);
        }
        PrimitiveType dateTypeItems = buildIfDateType(javaType, repetition, parquetFieldName);
        if (dateTypeItems != null) {
            return dateTypeItems;
        }
        if (type instanceof WriteRecordModelType<?> childWriteRecordType) {
            List<Type> childFields = buildChildFields(childWriteRecordType, visited);
            return new GroupType(repetition, parquetFieldName, childFields);
        }
        return null;
    }

    private List<Type> buildChildFields(WriteRecordModelType<?> writeRecordType, Set<WriteRecordModelType<?>> visited) {
        validateNotVisitedRecord(writeRecordType, visited);
        List<Type> fields = createGroupFields(writeRecordType, visited);
        visited.remove(writeRecordType);
        return fields;
    }

    private PrimitiveType buildIfPrimitiveType(FieldTypeInspect javaType, Repetition repetition, String name) {
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

    private PrimitiveType buildIfDateType(FieldTypeInspect javaType, Repetition repetition, String name) {
        if (javaType.isLocalDate()) {
            return primitive(PrimitiveTypeName.INT32, repetition).as(dateType()).named(name);
        }
        if (javaType.isLocalTime()) {
            TimeUnit timeUnit = carpetConfiguration.defaultTimeUnit();
            boolean isAdjustedToUTC = carpetConfiguration.defaultTimeIsAdjustedToUTC();
            LogicalTypeAnnotation timeType = timeType(isAdjustedToUTC, toParquetTimeUnit(timeUnit));
            var typeName = switch (timeUnit) {
            case MILLIS -> PrimitiveTypeName.INT32;
            case MICROS, NANOS -> PrimitiveTypeName.INT64;
            };
            return primitive(typeName, repetition).as(timeType).named(name);
        }
        if (javaType.isLocalDateTime()) {
            TimeUnit timeUnit = carpetConfiguration.defaultTimeUnit();
            var timeStampType = timestampType(false, toParquetTimeUnit(timeUnit));
            return primitive(PrimitiveTypeName.INT64, repetition).as(timeStampType).named(name);
        }
        if (javaType.isInstant()) {
            TimeUnit timeUnit = carpetConfiguration.defaultTimeUnit();
            var timeStampType = timestampType(true, toParquetTimeUnit(timeUnit));
            return primitive(PrimitiveTypeName.INT64, repetition).as(timeStampType).named(name);
        }
        return null;
    }

    private LogicalTypeAnnotation.TimeUnit toParquetTimeUnit(TimeUnit timeUnit) {
        return switch (timeUnit) {
        case MILLIS -> LogicalTypeAnnotation.TimeUnit.MILLIS;
        case MICROS -> LogicalTypeAnnotation.TimeUnit.MICROS;
        case NANOS -> LogicalTypeAnnotation.TimeUnit.NANOS;
        };
    }

    private Type buildDecimalTypeItem(Repetition repetition, String name) {
        DecimalConfig decimalConfig = carpetConfiguration.decimalConfig();
        if (decimalConfig == null) {
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

    private void validateNotVisitedRecord(WriteRecordModelType<?> recordClass, Set<WriteRecordModelType<?>> visited) {
        if (visited.contains(recordClass)) {
            throw new RecordTypeConversionException("Recusive records are not supported");
        }
        visited.add(recordClass);
    }

}