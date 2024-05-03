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
import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.intType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.uuidType;

import java.lang.reflect.RecordComponent;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
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

    public boolean validatePrimitiveCompatibility(PrimitiveType primitiveType, Class<?> javaType) {
        JavaType type = new JavaType(javaType);
        return switch (primitiveType.getPrimitiveTypeName()) {
        case INT32 -> validInt32Source(primitiveType, type);
        case INT64 -> validInt64Source(primitiveType, type);
        case FLOAT -> validFloatSource(primitiveType, type);
        case DOUBLE -> validDoubleSource(primitiveType, type);
        case BOOLEAN -> validBooleanSource(primitiveType, type);
        case BINARY -> validBinarySource(primitiveType, type);
        case FIXED_LEN_BYTE_ARRAY -> validFixedLenBinarySource(primitiveType, type);
        case INT96 -> throw new RecordTypeConversionException(type + " deserialization not supported");
        default -> false;
        };
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

    private boolean validInt32Source(PrimitiveType primitiveType, JavaType type) {
        if (type.isInteger() || type.isLong() || type.isDouble()) {
            return true;
        }
        if (!failNarrowingPrimitiveConversion) {
            if (type.isFloat() || type.isShort() || type.isByte()) {
                return true;
            }
        }

        LogicalTypeAnnotation logicalTypeAnnotation = primitiveType.getLogicalTypeAnnotation();
        if (logicalTypeAnnotation == null) {
            return throwInvalidConversionException(primitiveType, type);
        }
        if (type.isShort() && logicalTypeAnnotation.equals(INT16)) {
            return true;
        }
        if (type.isByte() && logicalTypeAnnotation.equals(INT8)) {
            return true;
        }
        if (type.isLocalDate() && logicalTypeAnnotation.equals(dateType())) {
            return true;
        }
        if (type.isLocalTime() && logicalTypeAnnotation instanceof TimeLogicalTypeAnnotation time
                && time.getUnit() == TimeUnit.MILLIS) {
            return true;
        }
        return throwInvalidConversionException(primitiveType, type);
    }

    private boolean validInt64Source(PrimitiveType primitiveType, JavaType type) {
        if (type.isLong()) {
            return true;
        }
        if (!failNarrowingPrimitiveConversion) {
            if (type.isInteger() || type.isDouble() || type.isFloat() || type.isShort() || type.isByte()) {
                return true;
            }
        }
        LogicalTypeAnnotation logicalTypeAnnotation = primitiveType.getLogicalTypeAnnotation();
        if (type.isLocalTime() && logicalTypeAnnotation instanceof TimeLogicalTypeAnnotation time
                && (time.getUnit() == TimeUnit.MICROS || time.getUnit() == TimeUnit.NANOS)) {
            return true;
        }
        if ((type.isLocalDateTime() || type.isInstant())
                && logicalTypeAnnotation instanceof TimestampLogicalTypeAnnotation timeStamp) {
            // TODO: add logic to fail about isAdjustedToUTC conversion
            return true;
        }
        return throwInvalidConversionException(primitiveType, type);
    }

    private boolean validFloatSource(PrimitiveType primitiveType, JavaType type) {
        if (type.isDouble() || type.isFloat()) {
            return true;
        }
        if (!failNarrowingPrimitiveConversion) {
        }
        return throwInvalidConversionException(primitiveType, type);
    }

    private boolean validDoubleSource(PrimitiveType primitiveType, JavaType type) {
        if (type.isDouble()) {
            return true;
        }
        if (!failNarrowingPrimitiveConversion) {
            if (type.isFloat()) {
                return true;
            }
        }
        return throwInvalidConversionException(primitiveType, type);
    }

    private boolean validBooleanSource(PrimitiveType primitiveType, JavaType type) {
        if (type.isBoolean()) {
            return true;
        }
        return throwInvalidConversionException(primitiveType, type);
    }

    private boolean validBinarySource(PrimitiveType primitiveType, JavaType type) {
        LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();
        if (stringType().equals(logicalType) && (type.isString() || type.isEnum())) {
            return true;
        }
        if (enumType().equals(logicalType) && (type.isString() || type.isEnum())) {
            return true;
        }
        return throwInvalidConversionException(primitiveType, type);
    }

    private boolean validFixedLenBinarySource(PrimitiveType parquetType, JavaType javaType) {
        if (uuidType().equals(parquetType.getLogicalTypeAnnotation())) {
            return validUuidSource(parquetType, javaType);
        }
        return false;
    }

    public static boolean isBasicSupportedType(JavaType type) {
        return (type.isInteger() || type.isLong() || type.isDouble() || type.isFloat()
                || type.isBoolean() || type.isShort() || type.isByte() || type.isEnum());
    }

    private boolean validUuidSource(PrimitiveType primitiveType, JavaType type) {
        if (type.isString() || type.isUuid()) {
            return true;
        }
        return throwInvalidConversionException(primitiveType, type);
    }

    private boolean throwInvalidConversionException(PrimitiveType primitiveType, JavaType type) {
        throw new RecordTypeConversionException(
                primitiveType.getPrimitiveTypeName().name() + " (" + primitiveType.getName()
                        + ") can not be converted to " + type.getTypeName());
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
