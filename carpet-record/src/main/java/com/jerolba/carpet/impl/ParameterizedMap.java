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
import java.lang.reflect.Type;

public class ParameterizedMap {

    private final Type mapType;
    private final AnnotatedType keyAnnotatedMapElementType;
    private final AnnotatedType valueAnnotatedMapElementType;

    public ParameterizedMap(Type mapType, AnnotatedParameterizedType type) {
        this.mapType = mapType;
        AnnotatedType[] annotatedActualTypeArguments = type.getAnnotatedActualTypeArguments();
        this.keyAnnotatedMapElementType = annotatedActualTypeArguments[0];
        this.valueAnnotatedMapElementType = annotatedActualTypeArguments[1];
    }

    public Class<?> getMapType() {
        return (Class<?>) mapType;
    }

    public Class<?> getValueActualType() {
        return Parameterized.getClassFromType(valueAnnotatedMapElementType.getType(), "in Map value");
    }

    public Class<?> getKeyActualType() {
        return Parameterized.getClassFromType(keyAnnotatedMapElementType.getType(), "in Map key");
    }

    public ParameterizedMap getValueTypeAsMap() {
        return Parameterized.getParameterizedMap(valueAnnotatedMapElementType);
    }

    public ParameterizedCollection getValueTypeAsCollection() {
        return Parameterized.getParameterizedCollection(valueAnnotatedMapElementType);
    }

    public boolean valueIsCollection() {
        return Parameterized.isCollection(valueAnnotatedMapElementType.getType());
    }

    public boolean valueIsMap() {
        return Parameterized.isMap(valueAnnotatedMapElementType.getType());
    }

    public boolean keyIsCollection() {
        return Parameterized.isCollection(keyAnnotatedMapElementType.getType());
    }

    public boolean keyIsMap() {
        return Parameterized.isMap(keyAnnotatedMapElementType.getType());
    }

}