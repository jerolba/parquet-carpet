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

import static java.lang.invoke.MethodType.methodType;

import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaConversionException;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

import com.jerolba.carpet.model.ToBooleanFunction;
import com.jerolba.carpet.model.ToByteFunction;
import com.jerolba.carpet.model.ToFloatFunction;
import com.jerolba.carpet.model.ToShortFunction;

public class Reflection {

    private static final ConcurrentHashMap<CacheKey, Function<Object, Object>> ACCESSOR_CACHE = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<CacheKey, Object> PRIMITIVE_ACCESSOR_CACHE = new ConcurrentHashMap<>();

    private Reflection() {
    }

    public static Function<Object, Object> recordAccessor(Class<?> targetClass, RecordComponent recordComponent) {
        return cachedFieldAccessor(targetClass, recordComponent.getName(), recordComponent.getType());
    }

    public static Function<Object, Object> recordAccessor(Class<?> targetClass, Field classField) {
        return cachedFieldAccessor(targetClass, classField.getName(), classField.getType());
    }

    private static class CacheKey {

        private final Class<?> targetClass;
        private final String fieldName;
        private final Class<?> fieldType;

        public CacheKey(Class<?> targetClass, String fieldName, Class<?> fieldType) {
            this.targetClass = targetClass;
            this.fieldName = fieldName;
            this.fieldType = fieldType;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) obj;
            return targetClass == cacheKey.targetClass &&
                    fieldName.equals(cacheKey.fieldName) &&
                    fieldType == cacheKey.fieldType;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(targetClass) ^
                    fieldName.hashCode() ^
                    System.identityHashCode(fieldType);
        }
    }

    private static Function<Object, Object> cachedFieldAccessor(Class<?> targetClass, String name, Class<?> fieldType) {
        CacheKey cacheKey = new CacheKey(targetClass, name, fieldType);
        return ACCESSOR_CACHE.computeIfAbsent(cacheKey, k -> fieldAccessor(targetClass, name, fieldType));
    }

    private static Function<Object, Object> fieldAccessor(Class<?> targetClass, String name, Class<?> fieldType) {
        try {
            Lookup lookup = MethodHandles.lookup();
            MethodHandle methodHandle = null;
            try {
                methodHandle = lookup.findVirtual(targetClass, name, methodType(fieldType));
            } catch (IllegalAccessException e) {
                try {
                    Lookup privateLookup = MethodHandles.privateLookupIn(targetClass, lookup);
                    methodHandle = privateLookup.findVirtual(targetClass, name, methodType(fieldType));
                    lookup = privateLookup;
                } catch (IllegalAccessException e1) {
                    return viaDeclaredMethod(targetClass, name, lookup);
                }
            }
            try {
                CallSite site = LambdaMetafactory.metafactory(lookup,
                        "apply",
                        methodType(Function.class),
                        methodType(Object.class, Object.class),
                        methodHandle,
                        methodType(fieldType, targetClass));
                return (Function<Object, Object>) site.getTarget().invokeExact();
            } catch (Throwable e) {
                return viaDeclaredMethod(targetClass, name, lookup);
            }
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static Function<Object, Object> viaDeclaredMethod(Class<?> targetClass, String name, Lookup lookup)
            throws NoSuchMethodException {
        try {
            Method m = targetClass.getDeclaredMethod(name);
            m.setAccessible(true);
            MethodHandle pmh = lookup.unreflect(m);
            return obj -> {
                try {
                    return pmh.invoke(obj);
                } catch (Throwable t2) {
                    throw new RuntimeException(t2);
                }
            };
        } catch (IllegalAccessException em) {
            throw new RuntimeException(em);
        }
    }

    public static Object intFieldAccessor(Class<?> targetClass, String name) {
        CacheKey cacheKey = new CacheKey(targetClass, name, int.class);
        return PRIMITIVE_ACCESSOR_CACHE.computeIfAbsent(cacheKey, k -> buildIntAccessor(targetClass, name));
    }

    private static ToIntFunction<Object> buildIntAccessor(Class<?> targetClass, String name) {
        try {
            var methodHandle = createMethodHandle(targetClass, name, int.class, "applyAsInt", ToIntFunction.class);
            return (ToIntFunction<Object>) methodHandle.invokeExact();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public static Object longFieldAccessor(Class<?> targetClass, String name) {
        CacheKey cacheKey = new CacheKey(targetClass, name, long.class);
        return PRIMITIVE_ACCESSOR_CACHE.computeIfAbsent(cacheKey, k -> buildLongAccessor(targetClass, name));
    }

    private static ToLongFunction<Object> buildLongAccessor(Class<?> targetClass, String name) {
        try {
            var methodHandle = createMethodHandle(targetClass, name, long.class, "applyAsLong", ToLongFunction.class);
            return (ToLongFunction<Object>) methodHandle.invokeExact();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public static Object floatFieldAccessor(Class<?> targetClass, String name) {
        CacheKey cacheKey = new CacheKey(targetClass, name, float.class);
        return PRIMITIVE_ACCESSOR_CACHE.computeIfAbsent(cacheKey, k -> buildFloatAccessor(targetClass, name));
    }

    private static ToFloatFunction<Object> buildFloatAccessor(Class<?> targetClass, String name) {
        try {
            var methodHandle = createMethodHandle(targetClass, name, float.class, "applyAsFloat",
                    ToFloatFunction.class);
            return (ToFloatFunction<Object>) methodHandle.invokeExact();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public static Object doubleFieldAccessor(Class<?> targetClass, String name) {
        CacheKey cacheKey = new CacheKey(targetClass, name, double.class);
        return PRIMITIVE_ACCESSOR_CACHE.computeIfAbsent(cacheKey, k -> buildDoubleAccessor(targetClass, name));
    }

    private static ToDoubleFunction<Object> buildDoubleAccessor(Class<?> targetClass, String name) {
        try {
            var methodHandle = createMethodHandle(targetClass, name, double.class, "applyAsDouble",
                    ToDoubleFunction.class);
            return (ToDoubleFunction<Object>) methodHandle.invokeExact();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public static Object byteFieldAccessor(Class<?> targetClass, String name) {
        CacheKey cacheKey = new CacheKey(targetClass, name, byte.class);
        return PRIMITIVE_ACCESSOR_CACHE.computeIfAbsent(cacheKey, k -> buildByteAccessor(targetClass, name));
    }

    private static ToByteFunction<Object> buildByteAccessor(Class<?> targetClass, String name) {
        try {
            var methodHandle = createMethodHandle(targetClass, name, byte.class, "applyAsByte", ToByteFunction.class);
            return (ToByteFunction<Object>) methodHandle.invokeExact();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public static Object shortFieldAccessor(Class<?> targetClass, String name) {
        CacheKey cacheKey = new CacheKey(targetClass, name, short.class);
        return PRIMITIVE_ACCESSOR_CACHE.computeIfAbsent(cacheKey, k -> buildShortAccessor(targetClass, name));
    }

    private static ToShortFunction<Object> buildShortAccessor(Class<?> targetClass, String name) {
        try {
            var methodHandle = createMethodHandle(targetClass, name, short.class, "applyAsShort",
                    ToShortFunction.class);
            return (ToShortFunction<Object>) methodHandle.invokeExact();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public static Object booleanFieldAccessor(Class<?> targetClass, String name) {
        CacheKey cacheKey = new CacheKey(targetClass, name, boolean.class);
        return PRIMITIVE_ACCESSOR_CACHE.computeIfAbsent(cacheKey, k -> buildBooleanAccessor(targetClass, name));
    }

    private static ToBooleanFunction<Object> buildBooleanAccessor(Class<?> targetClass, String name) {
        try {
            var methodHandle = createMethodHandle(targetClass, name, boolean.class, "applyAsBoolean",
                    ToBooleanFunction.class);
            return (ToBooleanFunction<Object>) methodHandle.invokeExact();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private static MethodHandle createMethodHandle(Class<?> targetClass, String name, Class<?> fieldType,
            String methodName, Class<?> extractorFunction) throws NoSuchMethodException, LambdaConversionException {
        LookupMethod lookupMethod = findRecordMethod(targetClass, name, fieldType);
        CallSite callSite = LambdaMetafactory.metafactory(
                lookupMethod.lookup(),
                methodName,
                methodType(extractorFunction),
                methodType(fieldType, Object.class),
                lookupMethod.methodHandle(),
                methodType(fieldType, targetClass));
        return callSite.getTarget();
    }

    private static final LookupMethod findRecordMethod(Class<?> targetClass, String name, Class<?> fieldType)
            throws NoSuchMethodException {
        Lookup lookup = MethodHandles.lookup();
        try {
            MethodHandle methodHandle = lookup.findVirtual(targetClass, name, methodType(fieldType));
            return new LookupMethod(lookup, methodHandle);
        } catch (IllegalAccessException e) {
            try {
                Lookup privateLookup = MethodHandles.privateLookupIn(targetClass, lookup);
                MethodHandle methodHandle = privateLookup.findVirtual(targetClass, name, methodType(fieldType));
                return new LookupMethod(privateLookup, methodHandle);
            } catch (IllegalAccessException e1) {
                throw new RuntimeException("Cannot access field: " + name, e1);
            }
        }
    }

    private record LookupMethod(Lookup lookup, MethodHandle methodHandle) {
    }

}