package com.jerolba.carpet.annotation;

import static java.lang.annotation.ElementType.RECORD_COMPONENT;
import static java.lang.annotation.ElementType.TYPE_USE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Retention(RUNTIME)
@Target({ RECORD_COMPONENT, TYPE_USE })
public @interface ParquetGeometry {

    String value() default "";
}
