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
package com.jerolba.carpet.impl;

import java.lang.reflect.AnnotatedParameterizedType;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiFunction;

import com.jerolba.carpet.RecordTypeConversionException;

public class Parameterized {

    public static ParameterizedCollection getParameterizedCollection(RecordComponent attr) {
        return getParameterizedCollection(attr.getAnnotatedType());
    }

    public static ParameterizedCollection getParameterizedCollection(Field attr) {
        return getParameterizedCollection(attr.getAnnotatedType());
    }

    public static ParameterizedCollection getParameterizedCollection(AnnotatedType annotatedTypeCollection) {
        return parametizeTo(annotatedTypeCollection, ParameterizedCollection::new);
    }

    public static boolean isCollection(Type type) {
        return typeIsAssignableFrom(type, Collection.class);
    }

    public static ParameterizedMap getParameterizedMap(RecordComponent attr) {
        return getParameterizedMap(attr.getAnnotatedType());
    }

    public static ParameterizedMap getParameterizedMap(Field attr) {
        return getParameterizedMap(attr.getAnnotatedType());
    }

    public static ParameterizedMap getParameterizedMap(AnnotatedType annotatedTypeMap) {
        return parametizeTo(annotatedTypeMap, ParameterizedMap::new);
    }

    public static boolean isMap(Type type) {
        return typeIsAssignableFrom(type, Map.class);
    }

    public static Class<?> getClassFromType(Type type, String usageForError) {
        if ((type instanceof Class<?> finalType)) {
            return finalType;
        }
        if ((type instanceof ParameterizedType parameterizedType)) {
            Type rawType = parameterizedType.getRawType();
            if (rawType instanceof Class<?> finalType) {
                return finalType;
            }
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
            if (collectionActualType instanceof Class<?> finalType) {
                return toAssign.isAssignableFrom(finalType);
            }
        }
        return false;
    }

    private static <T> T parametizeTo(AnnotatedType annotatedType,
            BiFunction<Type, AnnotatedParameterizedType, T> constructor) {
        if (annotatedType instanceof AnnotatedParameterizedType paramType) {
            if (paramType.getType() instanceof ParameterizedType rawType) {
                Type raw = rawType.getRawType();
                return constructor.apply(raw, paramType);
            }
        }
        throw new RecordTypeConversionException("Unsupported type in collection: " + annotatedType);
    }

}
