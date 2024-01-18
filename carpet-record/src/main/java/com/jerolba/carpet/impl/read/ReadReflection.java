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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class ReadReflection {

    public static class ConstructorParams {

        private final Constructor<?> constructor;
        private final Object[] defaultParamsValues;
        public final Object[] c;

        public ConstructorParams(Class<?> recordClass) {
            constructor = findConstructor(recordClass);
            RecordComponent[] components = recordClass.getRecordComponents();
            defaultParamsValues = createDefaultConstructorParams(components);
            c = new Object[components.length];
        }

        public Object create() {
            try {
                return constructor.newInstance(c);
            } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
                    | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }

        public void resetParams() {
            System.arraycopy(defaultParamsValues, 0, c, 0, defaultParamsValues.length);
        }

        private Object[] createDefaultConstructorParams(RecordComponent[] components) {
            Object[] defaultParams = new Object[components.length];
            for (int i = 0; i < components.length; i++) {
                defaultParams[i] = null;
                Class<?> type = components[i].getType();
                if (type.isPrimitive()) {
                    defaultParams[i] = getMissingParquetAttr(type);
                }
            }
            return defaultParams;
        }

        private static Object getMissingParquetAttr(Class<?> type) {
            return switch (type.getName()) {
            case "byte" -> (byte) 0;
            case "short" -> (short) 0;
            case "int" -> 0;
            case "long" -> 0L;
            case "double" -> 0.0;
            case "float" -> 0.0F;
            case "boolean" -> false;
            default -> null;
            };
        }

        private static Constructor<?> findConstructor(Class<?> recordClass) {
            Object[] componentsTypes = Stream.of(recordClass.getRecordComponents())
                    .map(RecordComponent::getType)
                    .toArray();
            Constructor<?>[] declaredConstructors = recordClass.getDeclaredConstructors();
            for (var c : declaredConstructors) {
                Class<?>[] parameterTypes = c.getParameterTypes();
                if (Arrays.equals(componentsTypes, parameterTypes, (c1, c2) -> c1.equals(c2) ? 0 : 1)) {
                    c.setAccessible(true);
                    return c;
                }
            }
            throw new RuntimeException(recordClass.getName() + " record has an invalid constructor");
        }

    }

    public static Supplier<Collection<Object>> collectionFactory(Class<?> type) {
        if (Set.class.isAssignableFrom(type)) {
            if (type.equals(Set.class)) {
                return HashSet::new;
            }
            if (type.equals(HashSet.class)) {
                return HashSet::new;
            }
            if (type.equals(LinkedHashSet.class)) {
                return LinkedHashSet::new;
            }
            return getDefaultConstructor(type);
        }
        if (List.class.isAssignableFrom(type)) {
            if (type.equals(List.class)) {
                return ArrayList::new;
            }
            if (type.equals(LinkedList.class)) {
                return LinkedList::new;
            }
            return getDefaultConstructor(type);

        }
        return ArrayList::new;
    }

    public static Supplier<Map<Object, Object>> mapFactory(Class<?> type) {
        if (Map.class.isAssignableFrom(type)) {
            if (type.equals(Map.class)) {
                return HashMap::new;
            }
            if (type.equals(LinkedHashMap.class)) {
                return LinkedHashMap::new;
            }
            if (type.equals(TreeMap.class)) {
                return TreeMap::new;
            }
            return getDefaultConstructor(type);
        }
        return HashMap::new;
    }

    public static <T> Supplier<T> getDefaultConstructor(Class<?> type) {
        try {
            Constructor<?> constructor = type.getConstructor();
            return () -> {
                try {
                    return (T) constructor.newInstance();
                } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
                        | InvocationTargetException e) {
                    throw new RuntimeException(type + " class can not be instantiated", e);
                }
            };
        } catch (NoSuchMethodException | SecurityException e) {
            throw new RuntimeException(type + " class doesn't have an empty constructor", e);
        }
    }

}
