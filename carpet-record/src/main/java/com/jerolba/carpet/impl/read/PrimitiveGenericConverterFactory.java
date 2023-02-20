package com.jerolba.carpet.impl.read;

import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;

import java.util.function.Consumer;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.read.converter.BooleanGenericConverter;
import com.jerolba.carpet.impl.read.converter.EnumGenericConverter;
import com.jerolba.carpet.impl.read.converter.FromDecimalToDoubleGenericConverter;
import com.jerolba.carpet.impl.read.converter.FromDecimalToFloatGenericConverter;
import com.jerolba.carpet.impl.read.converter.FromIntToByteGenericConverter;
import com.jerolba.carpet.impl.read.converter.FromIntToDoubleGenericConverter;
import com.jerolba.carpet.impl.read.converter.FromIntToIntegerGenericConverter;
import com.jerolba.carpet.impl.read.converter.FromIntToLongGenericConverter;
import com.jerolba.carpet.impl.read.converter.FromIntToShortGenericConverter;
import com.jerolba.carpet.impl.read.converter.StringGenericConverter;

class PrimitiveGenericConverterFactory {

    public static Converter buildPrimitiveGenericConverters(Type parquetField, Class<?> genericType,
            Consumer<Object> listConsumer) {
        PrimitiveTypeName type = parquetField.asPrimitiveType().getPrimitiveTypeName();
        return switch (type) {
        case INT32, INT64 -> genericBuildFromIntConverter(listConsumer, genericType);
        case FLOAT, DOUBLE -> genericBuildFromDecimalConverter(listConsumer, genericType);
        case BOOLEAN -> genericBuildFromBooleanConverter(listConsumer, genericType);
        case BINARY -> genericBuildFromBinaryConverter(listConsumer, genericType, parquetField);
        case INT96, FIXED_LEN_BYTE_ARRAY -> throw new RecordTypeConversionException(
                type + " deserialization not supported");
        default -> throw new RecordTypeConversionException(type + " deserialization not supported");
        };
    }

    public static Converter genericBuildFromIntConverter(Consumer<Object> listConsumer, Class<?> type) {
        String typeName = type.getName();
        if (typeName.equals("int") || typeName.equals("java.lang.Integer")) {
            return new FromIntToIntegerGenericConverter(listConsumer);
        }
        if (typeName.equals("long") || typeName.equals("java.lang.Long")) {
            return new FromIntToLongGenericConverter(listConsumer);
        }
        if (typeName.equals("short") || typeName.equals("java.lang.Short")) {
            return new FromIntToShortGenericConverter(listConsumer);
        }
        if (typeName.equals("byte") || typeName.equals("java.lang.Byte")) {
            return new FromIntToByteGenericConverter(listConsumer);
        }
        if (typeName.equals("double") || typeName.equals("java.lang.Double")) {
            return new FromIntToDoubleGenericConverter(listConsumer);
        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + type.getName() + " collection");
    }

    public static Converter genericBuildFromDecimalConverter(Consumer<Object> listConsumer, Class<?> type) {
        String typeName = type.getName();
        if (typeName.equals("float") || typeName.equals("java.lang.Float")) {
            return new FromDecimalToFloatGenericConverter(listConsumer);
        }
        if (typeName.equals("double") || typeName.equals("java.lang.Double")) {
            return new FromDecimalToDoubleGenericConverter(listConsumer);
        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + type.getName() + " collection");
    }

    public static Converter genericBuildFromBooleanConverter(Consumer<Object> listConsumer, Class<?> type) {
        String typeName = type.getName();
        if (typeName.equals("boolean") || typeName.equals("java.lang.Boolean")) {
            return new BooleanGenericConverter(listConsumer);
        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + type.getName() + " collection");
    }

    public static Converter genericBuildFromBinaryConverter(Consumer<Object> listConsumer, Class<?> type,
            Type schemaType) {
        LogicalTypeAnnotation logicalType = schemaType.getLogicalTypeAnnotation();
        String typeName = type.getName();
        if (logicalType.equals(stringType())) {
            if (typeName.equals("java.lang.String")) {
                return new StringGenericConverter(listConsumer);
            }
            throw new RecordTypeConversionException(typeName + " not compatible with String field");
        }
        if (logicalType.equals(enumType())) {
            if (typeName.equals("java.lang.String")) {
                return new StringGenericConverter(listConsumer);
            }
            return new EnumGenericConverter(listConsumer, type);

        }
        throw new RecordTypeConversionException(
                typeName + " not compatible with " + schemaType.getName() + " field");
    }

}
