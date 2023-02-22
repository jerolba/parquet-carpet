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
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.util.function.Function;

public class Reflection {

    public static Function<Object, Object> recordAccessor(Class<?> targetClass, RecordComponent recordComponent) {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        try {
            MethodHandle findVirtual = lookup.findVirtual(targetClass, recordComponent.getName(),
                    methodType(recordComponent.getType()));
            CallSite site = LambdaMetafactory.metafactory(lookup,
                    "apply",
                    methodType(Function.class),
                    methodType(Object.class, Object.class),
                    findVirtual,
                    methodType(recordComponent.getType(), targetClass));
            return (Function<Object, Object>) site.getTarget().invokeExact();
        } catch (IllegalAccessException e) {
            try {
                Method m = targetClass.getDeclaredMethod(recordComponent.getName());
                m.setAccessible(true);
                MethodHandle pmh = lookup.unreflect(m);
                return obj -> {
                    try {
                        return pmh.invoke(obj);
                    } catch (Throwable e1) {
                        throw new RuntimeException(e1);
                    }
                };
            } catch (NoSuchMethodException | IllegalAccessException em) {
                throw new RuntimeException(em);
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

}
