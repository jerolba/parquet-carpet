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
import java.util.Arrays;
import java.util.stream.Stream;

public class ReadReflection {

    public static class ConstructorParams {

        private final Constructor<?> constructor;
        public final Object[] c;

        public ConstructorParams(Class<?> recordClass) {
            constructor = findConstructor(recordClass);
            RecordComponent[] components = recordClass.getRecordComponents();
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
