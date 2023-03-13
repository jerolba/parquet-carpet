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

import java.lang.reflect.RecordComponent;

public class JavaType {

    private final String typeName;
    private final Class<?> type;

    public JavaType(RecordComponent recordComponent) {
        this(recordComponent.getType());
    }

    public JavaType(Class<?> type) {
        this.type = type;
        this.typeName = type.getName();
    }

    public Class<?> getJavaType() {
        return type;
    }

    public boolean isString() {
        return typeName.equals("java.lang.String");
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

    public String getTypeName() {
        return typeName;
    }

    public boolean isEnum() {
        return type.isEnum();
    }

    public boolean isRecord() {
        return type.isRecord();
    }

}
