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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class ParameterizedCollection {

    private final Type collectionType;
    private final Type collectionElementType;

    public ParameterizedCollection(Type collectionType, ParameterizedType type) {
        this.collectionType = collectionType;
        this.collectionElementType = type.getActualTypeArguments()[0];
    }

    public Class<?> getActualType() {
        return Parameterized.getClassFromType(collectionElementType, "in Collection");
    }

    public Class<?> getCollectionType() {
        return (Class<?>) collectionType;
    }

    public ParameterizedCollection getParametizedAsCollection() {
        if (collectionElementType instanceof ParameterizedType paramType) {
            Type collection = paramType.getRawType();
            return new ParameterizedCollection(collection, paramType);
        }
        return null;
    }

    public ParameterizedMap getParametizedAsMap() {
        if (collectionElementType instanceof ParameterizedType paramType) {
            Type map = paramType.getRawType();
            return new ParameterizedMap(map, paramType);
        }
        return null;
    }

    public boolean isCollection() {
        return Parameterized.isCollection(collectionElementType);
    }

    public boolean isMap() {
        return Parameterized.isMap(collectionElementType);
    }

}