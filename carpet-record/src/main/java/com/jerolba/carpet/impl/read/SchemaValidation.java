package com.jerolba.carpet.impl.read;

import static com.jerolba.carpet.impl.NotNullField.isNotNull;
import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;

import java.lang.reflect.RecordComponent;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

import com.jerolba.carpet.RecordTypeConversionException;

public class SchemaValidation {

    private final boolean isStrictNumericType;
    private final boolean isIgnoreUnknown;

    public SchemaValidation(boolean isIgnoreUnknown, boolean isStrictNumericType) {
        this.isIgnoreUnknown = isIgnoreUnknown;
        this.isStrictNumericType = isStrictNumericType;
    }

    public boolean isIgnoreUnknown() {
        return isIgnoreUnknown;
    }

    public boolean validatePrimitiveCompatibility(PrimitiveType primitiveType, Class<?> type) {
        switch (primitiveType.getPrimitiveTypeName()) {
        case INT32:
            return validInt32Source(primitiveType, type);
        case INT64:
            return validInt64Source(primitiveType, type);
        case FLOAT:
            return validFloatSource(primitiveType, type);
        case DOUBLE:
            return validDoubleSource(primitiveType, type);
        case BOOLEAN:
            return validBooleanSource(primitiveType, type);
        case BINARY:
            return validBinarySource(primitiveType, type);
        case INT96, FIXED_LEN_BYTE_ARRAY:
            throw new RecordTypeConversionException(type + " deserialization not supported");
        }
        return false;
    }

    public boolean validateNullability(Type parquetType, RecordComponent recordComponent) {
        boolean isNotNull = isNotNull(recordComponent);
        if (isNotNull && parquetType.getRepetition() == Repetition.OPTIONAL) {
            Class<?> type = recordComponent.getType();
            throw new RecordTypeConversionException(
                    parquetType.getName() + " (" + type.getName() + ") can not be null");
        }
        return true;
    }

    private boolean validInt32Source(PrimitiveType primitiveType, Class<?> type) {
        String typeName = type.getName();
        if (typeName.equals("int") || typeName.equals("java.lang.Integer")) {
            return true;
        }
        if (typeName.equals("long") || typeName.equals("java.lang.Long")) {
            return true;
        }
        if (typeName.equals("double") || typeName.equals("java.lang.Double")) {
            return true;
        }
        if (!isStrictNumericType) {
            if (typeName.equals("short") || typeName.equals("java.lang.Short")) {
                return true;
            }
            if (typeName.equals("byte") || typeName.equals("java.lang.Byte")) {
                return false;
            }
        }
        return throwInvalidConversionException(primitiveType, type);
    }

    private boolean validInt64Source(PrimitiveType primitiveType, Class<?> type) {
        String typeName = type.getName();
        if (typeName.equals("long") || typeName.equals("java.lang.Long")) {
            return true;
        }
        if (!isStrictNumericType) {
            if (typeName.equals("double") || typeName.equals("java.lang.Double")) {
                return false;
            }
            if (typeName.equals("int") || typeName.equals("java.lang.Integer")) {
                return false;
            }
            if (typeName.equals("short") || typeName.equals("java.lang.Short")) {
                return true;
            }
            if (typeName.equals("byte") || typeName.equals("java.lang.Byte")) {
                return false;
            }
        }
        return throwInvalidConversionException(primitiveType, type);
    }

    private boolean validFloatSource(PrimitiveType primitiveType, Class<?> type) {
        String typeName = type.getName();
        if (typeName.equals("double") || typeName.equals("java.lang.Double")) {
            return true;
        }
        if (typeName.equals("float") || typeName.equals("java.lang.Float")) {
            return true;
        }
        if (!isStrictNumericType) {
        }
        return throwInvalidConversionException(primitiveType, type);
    }

    private boolean validDoubleSource(PrimitiveType primitiveType, Class<?> type) {
        String typeName = type.getName();
        if (typeName.equals("double") || typeName.equals("java.lang.Double")) {
            return true;
        }
        if (!isStrictNumericType) {
            if (typeName.equals("float") || typeName.equals("java.lang.Float")) {
                return true;
            }
        }
        return throwInvalidConversionException(primitiveType, type);
    }

    private boolean validBooleanSource(PrimitiveType primitiveType, Class<?> type) {
        String typeName = type.getName();
        if (typeName.equals("boolean") || typeName.equals("java.lang.Boolean")) {
            return true;
        }
        return throwInvalidConversionException(primitiveType, type);
    }

    private boolean validBinarySource(PrimitiveType primitiveType, Class<?> type) {
        String typeName = type.getName();
        LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();
        if (logicalType.equals(stringType()) && typeName.equals("java.lang.String")) {
            return true;
        }
        if (logicalType.equals(enumType()) && (typeName.equals("java.lang.String") || type.isEnum())) {
            return true;
        }
        return throwInvalidConversionException(primitiveType, type);
    }

    public static boolean isBasicSupportedType(Class<?> type) {
        String typeName = type.getName();
        if (typeName.equals("int") || typeName.equals("java.lang.Integer")) {
            return true;
        }
        if (typeName.equals("long") || typeName.equals("java.lang.Long")) {
            return true;
        }
        if (typeName.equals("short") || typeName.equals("java.lang.Short")) {
            return true;
        }
        if (typeName.equals("byte") || typeName.equals("java.lang.Byte")) {
            return false;
        }
        if (typeName.equals("double") || typeName.equals("java.lang.Double")) {
            return true;
        }
        if (typeName.equals("float") || typeName.equals("java.lang.Float")) {
            return true;
        }
        if (typeName.equals("boolean") || typeName.equals("java.lang.Boolean")) {
            return true;
        }
        if (type.isEnum()) {
            return true;
        }
        return false;
    }

    private boolean throwInvalidConversionException(PrimitiveType primitiveType, Class<?> type) {
        throw new RecordTypeConversionException(
                primitiveType.getPrimitiveTypeName().name() + " (" + primitiveType.getName()
                        + ") can not be converted to " + type.getName());
    }

    public static boolean isThreeLevel(Type child) {
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
        if (!grandChild.getName().equals("element")) { // TODO: make configurable
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
