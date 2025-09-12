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
package com.jerolba.carpet.impl.read;

import static com.jerolba.carpet.impl.NotNullField.isNotNull;
import static org.apache.parquet.schema.LogicalTypeAnnotation.bsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.jsonType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.uuidType;

import java.lang.reflect.RecordComponent;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.GeographyLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.GeometryLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

import com.jerolba.carpet.CarpetMissingColumnException;
import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.JavaType;

public class SchemaValidation {

    private static final IntLogicalTypeAnnotation INT8 = intType(8, true);
    private static final IntLogicalTypeAnnotation INT16 = intType(16, true);

    private final boolean failNarrowingPrimitiveConversion;
    private final boolean failOnMissingColumn;
    private final boolean failOnNullForPrimitives;

    public SchemaValidation(boolean failOnMissingColumn, boolean failNarrowingPrimitiveConversion,
            boolean failOnNullForPrimitives) {
        this.failOnMissingColumn = failOnMissingColumn;
        this.failNarrowingPrimitiveConversion = failNarrowingPrimitiveConversion;
        this.failOnNullForPrimitives = failOnNullForPrimitives;
    }

    public boolean validateMissingColumn(Class<?> clazz, String fieldName) {
        if (failOnMissingColumn) {
            throw new CarpetMissingColumnException("Field '" + fieldName + "' from class '"
                    + clazz.getName() + "' not present in parquet schema");
        }
        return true;
    }

    public boolean validatePrimitiveCompatibility(PrimitiveType primitiveType, JavaType type) {
        LogicalTypeAnnotation logicalTypeAnnotation = primitiveType.getLogicalTypeAnnotation();
        if (logicalTypeAnnotation != null && validLogicalTypeAnnotation(primitiveType, type)) {
            return true;
        }

        boolean valid = switch (primitiveType.getPrimitiveTypeName()) {
        case INT32 -> validInt32Source(type);
        case INT64 -> validInt64Source(type);
        case FLOAT -> validFloatSource(type);
        case DOUBLE -> validDoubleSource(type);
        case BOOLEAN -> validBooleanSource(type);
        case BINARY -> validBinarySource(type);
        case FIXED_LEN_BYTE_ARRAY, INT96 -> throwInvalidConversionException(primitiveType, type);
        default -> false;
        };
        if (!valid) {
            return throwInvalidConversionException(primitiveType, type);
        }
        return valid;
    }

    public boolean validateNullability(Type parquetType, RecordComponent recordComponent) {
        if (failOnNullForPrimitives) {
            boolean isNotNull = isNotNull(recordComponent);
            if (isNotNull && parquetType.getRepetition() == Repetition.OPTIONAL) {
                Class<?> type = recordComponent.getType();
                throw new RecordTypeConversionException(
                        "\"" + parquetType.getName() + "\" (" + type.getName() + ") on class \""
                                + recordComponent.getDeclaringRecord().getName() + "\" can not be null");
            }
        }
        return true;
    }

    private boolean validInt32Source(JavaType type) {
        if (type.isInteger() || type.isLong() || type.isDouble()) {
            return true;
        }
        if (!failNarrowingPrimitiveConversion) {
            return (type.isFloat() || type.isShort() || type.isByte());
        }
        return false;
    }

    private boolean validInt64Source(JavaType type) {
        if (type.isLong()) {
            return true;
        }
        if (!failNarrowingPrimitiveConversion) {
            return (type.isInteger() || type.isDouble() || type.isFloat() || type.isShort() || type.isByte());
        }
        return false;
    }

    private boolean validFloatSource(JavaType type) {
        if (type.isDouble() || type.isFloat()) {
            return true;
        }
        if (!failNarrowingPrimitiveConversion) {
        }
        return false;
    }

    private boolean validDoubleSource(JavaType type) {
        if (type.isDouble()) {
            return true;
        }
        if (!failNarrowingPrimitiveConversion) {
            return type.isFloat();
        }
        return false;
    }

    private boolean validBooleanSource(JavaType type) {
        return type.isBoolean();
    }

    private boolean validBinarySource(JavaType type) {
        return type.isBinary();
    }

    public static boolean isBasicSupportedType(JavaType type) {
        return (type.isInteger() || type.isLong() || type.isDouble() || type.isFloat()
                || type.isBoolean() || type.isShort() || type.isByte() || type.isEnum());
    }

    private boolean validLogicalTypeAnnotation(PrimitiveType primitiveType, JavaType type) {
        var logicalType = primitiveType.getLogicalTypeAnnotation();
        var name = primitiveType.getPrimitiveTypeName();

        if (stringType().equals(logicalType) && (type.isString() || type.isEnum() || type.isBinary())) {
            return name == PrimitiveTypeName.BINARY;
        }
        if (enumType().equals(logicalType) && (type.isString() || type.isEnum() || type.isBinary())) {
            return name == PrimitiveTypeName.BINARY;
        }
        if (jsonType().equals(logicalType) && (type.isString() || type.isBinary())) {
            return name == PrimitiveTypeName.BINARY;
        }
        if (bsonType().equals(logicalType) && type.isBinary()) {
            return name == PrimitiveTypeName.BINARY;
        }
        if (logicalType instanceof GeometryLogicalTypeAnnotation && type.isGeometry()) {
            return name == PrimitiveTypeName.BINARY;
        }
        if (logicalType instanceof GeographyLogicalTypeAnnotation && type.isGeometry()) {
            return name == PrimitiveTypeName.BINARY;
        }
        if (logicalType.equals(uuidType()) && (type.isString() || type.isUuid())) {
            return name == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
        }
        if (logicalType.equals(INT16) && type.isShort()) {
            return name == PrimitiveTypeName.INT32;
        }
        if (type.isByte() && logicalType.equals(INT8)) {
            return name == PrimitiveTypeName.INT32;
        }
        if (logicalType instanceof DecimalLogicalTypeAnnotation && type.isBigDecimal()) {
            return name == PrimitiveTypeName.INT32
                    || name == PrimitiveTypeName.INT64
                    || name == PrimitiveTypeName.BINARY
                    || name == PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
        }
        if (type.isLocalDate() && logicalType.equals(dateType())) {
            return name == PrimitiveTypeName.INT32;
        }
        if (type.isLocalTime() && logicalType instanceof TimeLogicalTypeAnnotation time) {
            if (time.getUnit() == TimeUnit.MILLIS) {
                return name == PrimitiveTypeName.INT32;
            }
            if (time.getUnit() == TimeUnit.MICROS || time.getUnit() == TimeUnit.NANOS) {
                return name == PrimitiveTypeName.INT64;
            }
        }
        if (logicalType instanceof TimestampLogicalTypeAnnotation && (type.isLocalDateTime() || type.isInstant())) {
            // TODO: add logic to fail about isAdjustedToUTC conversion
            return name == PrimitiveTypeName.INT64;
        }
        return false;
    }

    private boolean throwInvalidConversionException(PrimitiveType primitiveType, JavaType type) {
        throw new RecordTypeConversionException(
                "Parquet '" + primitiveType + "' can not be converted to '" + type.getTypeName() + "'");
    }

    public static boolean isThreeLevel(Type child) {
        return isCompliantThreeLevelWith(child, "element") || isCompliantThreeLevelWith(child, "item");
    }

    public static boolean isCompliantThreeLevelWith(Type child, String elementName) {
//      <list-repetition> group <name> (LIST) {
//        repeated group list { <--child
//          <element-repetition> <element-type> element;
//        }
//     }
        if (child.isPrimitive()) {
            return false;
        }
        GroupType asGroup = child.asGroupType();
        if (!asGroup.getName().equals("list")) { // TODO: make configurable
            return false;
        }
        if (asGroup.getFieldCount() > 1) {
            return false;
        }
        Type grandChild = asGroup.getFields().get(0);
        if (!grandChild.getName().equals(elementName)) {
            return false;
        }
        return true;
    }

    public static boolean hasMapShape(GroupType rootGroup) {
//     optional group ids (MAP) {
//       repeated group key_value {
//         required binary key (STRING);
//         optional int32 value;
//       }
//     }
        if (rootGroup.getFieldCount() != 1) {
            return false;
        }
        Type keyValueType = rootGroup.getFields().get(0);
        if (!keyValueType.isRepetition(Repetition.REPEATED)) {
            return false;
        }
        if (keyValueType.isPrimitive()) {
            return false;
        }
        GroupType keyValueGroup = keyValueType.asGroupType();
        if (keyValueGroup.getFieldCount() != 2) {
            return false;
        }
        Type key = keyValueGroup.getFields().get(0);
        if (!key.isRepetition(Repetition.REQUIRED)) {
            return false;
        }
        return true;
    }

}
