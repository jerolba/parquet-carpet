package com.jerolba.carpet.impl;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

import com.jerolba.carpet.RecordTypeConversionException;

public class Parameterized {

    public static ParameterizedCollection getParameterizedCollection(RecordComponent attr) {
        return parametizeTo(attr, ParameterizedCollection::new);
    }

    public static boolean isCollection(Type type) {
        return typeIsAssignableFrom(type, Collection.class);
    }

    public static ParameterizedMap getParameterizedMap(RecordComponent attr) {
        return parametizeTo(attr, ParameterizedMap::new);
    }

    public static boolean isMap(Type type) {
        return typeIsAssignableFrom(type, Map.class);
    }

    public static Class<?> getClassFromType(Type type, String usageForError) {
        if ((type instanceof Class<?> finalType)) {
            return finalType;
        }
        if ((type instanceof TypeVariable<?> finalType)) {
            throw new RecordTypeConversionException(
                    finalType.getName() + " generic type not supported " + usageForError);
        }
        throw new RecordTypeConversionException("Invalid type " + type + " " + usageForError);
    }

    private static boolean typeIsAssignableFrom(Type type, Class<?> toAssign) {
        if (type instanceof ParameterizedType paramType) {
            Type collectionActualType = paramType.getRawType();
            if ((collectionActualType instanceof Class<?> finalType)) {
                if (toAssign.isAssignableFrom(finalType)) {
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }
        return false;
    }

    private static <T> T parametizeTo(RecordComponent attr, Function<ParameterizedType, T> constructor) {
        java.lang.reflect.Type genericType = attr.getGenericType();
        if (genericType instanceof TypeVariable<?>) {
            throw new RecordTypeConversionException(genericType.toString() + " generic types not supported");
        }
        if (genericType instanceof ParameterizedType paramType) {
            return constructor.apply(paramType);
        }
        throw new RecordTypeConversionException("Unsuported type in collection");
    }

}
