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

import java.lang.annotation.Annotation;
import java.lang.reflect.RecordComponent;
import java.util.Collection;
import java.util.Map;

import org.apache.parquet.io.api.Binary;
import org.locationtech.jts.geom.Geometry;

public class JavaType {

    private final String typeName;
    private final Class<?> type;
    private final Annotation[] declaredAnnotations;

    public JavaType(RecordComponent recordComponent) {
        this(recordComponent.getType(), recordComponent.getDeclaredAnnotations());
    }

    public JavaType(Class<?> type) {
        this(type, null);
    }

    public JavaType(Class<?> type, Annotation[] declaredAnnotations) {
        this.type = type;
        this.typeName = type.getName();
        this.declaredAnnotations = declaredAnnotations;
    }

    public Class<?> getJavaType() {
        return type;
    }

    public boolean isJavaPrimitive() {
        return type.isPrimitive();
    }

    public boolean isString() {
        return typeName.equals("java.lang.String");
    }

    public boolean isBinary() {
        return Binary.class.isAssignableFrom(type);
    }

    public boolean isInteger() {
        return (typeName.equals("int") || typeName.equals("java.lang.Integer"));
    }

    public boolean isLong() {
        return typeName.equals("long") || typeName.equals("java.lang.Long");
    }

    public boolean isShort() {
        return typeName.equals("short") || typeName.equals("java.lang.Short");
    }

    public boolean isByte() {
        return typeName.equals("byte") || typeName.equals("java.lang.Byte");
    }

    public boolean isDouble() {
        return typeName.equals("double") || typeName.equals("java.lang.Double");
    }

    public boolean isFloat() {
        return typeName.equals("float") || typeName.equals("java.lang.Float");
    }

    public boolean isBoolean() {
        return typeName.equals("boolean") || typeName.equals("java.lang.Boolean");
    }

    public boolean isUuid() {
        return typeName.equals("java.util.UUID");
    }

    public boolean isBigDecimal() {
        return typeName.equals("java.math.BigDecimal");
    }

    public String getTypeName() {
        return typeName;
    }

    public boolean isEnum() {
        return type.isEnum();
    }

    public boolean isLocalDate() {
        return typeName.equals("java.time.LocalDate");
    }

    public boolean isLocalTime() {
        return typeName.equals("java.time.LocalTime");
    }

    public boolean isLocalDateTime() {
        return typeName.equals("java.time.LocalDateTime");
    }

    public boolean isInstant() {
        return typeName.equals("java.time.Instant");
    }

    public boolean isGeometry() {
        return typeName.equals("org.locationtech.jts.geom.Geometry") || Geometry.class.isAssignableFrom(type);
    }

    public boolean isVariant() {
        return typeName.equals("org.apache.parquet.variant.Variant");
    }

    public boolean isRecord() {
        return type.isRecord();
    }

    public boolean isCollection() {
        return Collection.class.isAssignableFrom(type);
    }

    public boolean isMap() {
        return Map.class.isAssignableFrom(type);
    }

    public <T extends Annotation> boolean isAnnotatedWith(Class<T> annotationClass) {
        return getAnnotation(annotationClass) != null;
    }

    public <T extends Annotation> T getAnnotation(Class<T> annotationClass) {
        if (declaredAnnotations == null) {
            return null;
        }
        for (Annotation annotation : declaredAnnotations) {
            if (annotation.annotationType().equals(annotationClass)) {
                return (T) annotation;
            }
        }
        return null;
    }

    public Annotation[] getDeclaredAnnotations() {
        return declaredAnnotations;
    }

}
