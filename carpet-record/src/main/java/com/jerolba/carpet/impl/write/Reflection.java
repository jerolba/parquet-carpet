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
