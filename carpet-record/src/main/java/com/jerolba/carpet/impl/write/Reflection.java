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
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class Reflection {

    private static final ConcurrentHashMap<CacheKey, Function<Object, Object>> ACCESSOR_CACHE = new ConcurrentHashMap<>();

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
            Lookup lookup = null;
            MethodHandle findVirtual = null;
            try {
                lookup = MethodHandles.lookup();
                findVirtual = lookup.findVirtual(targetClass, name, methodType(fieldType));
            } catch (IllegalAccessException e) {
                try {
                    Lookup privateLookup = MethodHandles.privateLookupIn(targetClass, lookup);
                    findVirtual = privateLookup.findVirtual(targetClass, name, methodType(fieldType));
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
                        findVirtual,
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

}