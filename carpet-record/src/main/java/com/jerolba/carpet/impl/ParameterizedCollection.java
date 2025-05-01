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
import java.lang.reflect.AnnotatedParameterizedType;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Type;

public class ParameterizedCollection {

    private final Type collectionType;
    private final Type collectionElementType;
    private final AnnotatedType annotatedCollectionElementType;

    public ParameterizedCollection(Type collectionType, AnnotatedParameterizedType type) {
        this.collectionType = collectionType;
        this.annotatedCollectionElementType = type.getAnnotatedActualTypeArguments()[0];
        this.collectionElementType = annotatedCollectionElementType.getType();
    }

    public Class<?> getActualType() {
        return Parameterized.getClassFromType(collectionElementType, "in Collection");
    }

    public JavaType getActualJavaType() {
        return new JavaType(getActualType(), getAnnotations());
    }

    public Class<?> getCollectionType() {
        return (Class<?>) collectionType;
    }

    public ParameterizedCollection getParametizedAsCollection() {
        return Parameterized.getParameterizedCollection(annotatedCollectionElementType);
    }

    public ParameterizedMap getParametizedAsMap() {
        return Parameterized.getParameterizedMap(annotatedCollectionElementType);
    }

    public boolean isCollection() {
        return Parameterized.isCollection(collectionElementType);
    }

    public boolean isMap() {
        return Parameterized.isMap(collectionElementType);
    }

    private Annotation[] getAnnotations() {
        return annotatedCollectionElementType.getDeclaredAnnotations();
    }

}