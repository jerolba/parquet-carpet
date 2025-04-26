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

import static com.jerolba.carpet.impl.write.SchemaBuilder.buildDecimalTypeItem;
import static com.jerolba.carpet.impl.write.SchemaBuilder.buildInstantType;
import static com.jerolba.carpet.impl.write.SchemaBuilder.buildLocalDateTimeType;
import static com.jerolba.carpet.impl.write.SchemaBuilder.buildLocalDateType;
import static com.jerolba.carpet.impl.write.SchemaBuilder.buildLocalTimeType;
import static com.jerolba.carpet.impl.write.SchemaBuilder.buildUuidType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.bsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.jsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
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
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.model.BinaryType.BinaryLogicalType;
import com.jerolba.carpet.model.CollectionType;
import com.jerolba.carpet.model.EnumType.EnumLogicalType;
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
            return buildStringType(javaType.stringLogicalType(), repetition, parquetFieldName);
        } else if (javaType.isBinary()) {
            return buildBinaryType(javaType.binaryLogicalType(), repetition, parquetFieldName);
        } else if (javaType.isEnum()) {
            return buildEnumType(javaType.enumLogicalType(), repetition, parquetFieldName);
        } else if (javaType.isUuid()) {
            return buildUuidType(repetition, parquetFieldName);
        } else if (javaType.isBigDecimal()) {
            return buildDecimalTypeItem(repetition, parquetFieldName, carpetConfiguration.decimalConfig());
        } else if (javaType.isLocalDate()) {
            return buildLocalDateType(repetition, parquetFieldName);
        } else if (javaType.isLocalTime()) {
            return buildLocalTimeType(repetition, parquetFieldName,
                    carpetConfiguration.defaultTimeUnit(), carpetConfiguration.defaultTimeIsAdjustedToUTC());
        } else if (javaType.isLocalDateTime()) {
            return buildLocalDateTimeType(repetition, parquetFieldName, carpetConfiguration.defaultTimeUnit());
        } else if (javaType.isInstant()) {
            return buildInstantType(repetition, parquetFieldName, carpetConfiguration.defaultTimeUnit());
        } else if (type instanceof WriteRecordModelType<?> childWriteRecordType) {
            List<Type> childFields = buildChildFields(childWriteRecordType, visited);
            return new GroupType(repetition, parquetFieldName, childFields);
        }
        return null;
    }

    private Type buildStringType(StringLogicalType stringLogicalType, Repetition repetition, String parquetFieldName) {
        var binary = primitive(BINARY, repetition);
        if (stringLogicalType == null) {
            return binary.as(stringType()).named(parquetFieldName);
        }
        return switch (stringLogicalType) {
        case JSON -> binary.as(jsonType()).named(parquetFieldName);
        case ENUM -> binary.as(enumType()).named(parquetFieldName);
        case STRING -> binary.as(stringType()).named(parquetFieldName);
        };
    }

    private Type buildBinaryType(BinaryLogicalType binaryLogicalType, Repetition repetition, String parquetFieldName) {
        var binary = primitive(BINARY, repetition);
        if (binaryLogicalType == null) {
            return binary.named(parquetFieldName);
        }
        return switch (binaryLogicalType) {
        case STRING -> binary.as(stringType()).named(parquetFieldName);
        case ENUM -> binary.as(enumType()).named(parquetFieldName);
        case JSON -> binary.as(jsonType()).named(parquetFieldName);
        case BSON -> binary.as(bsonType()).named(parquetFieldName);
        };
    }

    private Type buildEnumType(EnumLogicalType logicalType, Repetition repetition, String parquetFieldName) {
        var binary = primitive(BINARY, repetition);
        if (logicalType == null) {
            return binary.as(enumType()).named(parquetFieldName);
        }
        return switch (logicalType) {
        case STRING -> binary.as(stringType()).named(parquetFieldName);
        case ENUM -> binary.as(enumType()).named(parquetFieldName);
        };
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

}