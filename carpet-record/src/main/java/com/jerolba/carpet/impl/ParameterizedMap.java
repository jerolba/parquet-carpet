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

public class ParameterizedMap {

    private final Type mapType;
    private final Type keyType;
    private final Type valueType;

    public ParameterizedMap(Type mapType, ParameterizedType type) {
        this.mapType = mapType;
        this.keyType = type.getActualTypeArguments()[0];
        this.valueType = type.getActualTypeArguments()[1];
    }

    public Class<?> getMapType() {
        return (Class<?>) mapType;
    }

    public Class<?> getValueActualType() {
        return Parameterized.getClassFromType(valueType, "in Map value");
    }

    public Class<?> getKeyActualType() {
        return Parameterized.getClassFromType(keyType, "in Map key");
    }

    public ParameterizedMap getValueTypeAsMap() {
        if (valueType instanceof ParameterizedType paramType) {
            Type map = paramType.getRawType();
            return new ParameterizedMap(map, paramType);
        }
        return null;
    }

    public ParameterizedCollection getValueTypeAsCollection() {
        if (valueType instanceof ParameterizedType paramType) {
            Type collection = paramType.getRawType();
            return new ParameterizedCollection(collection, paramType);
        }
        return null;
    }

    public boolean valueIsCollection() {
        return Parameterized.isCollection(valueType);
    }

    public boolean valueIsMap() {
        return Parameterized.isMap(valueType);
    }

    public boolean keyIsCollection() {
        return Parameterized.isCollection(keyType);
    }

    public boolean keyIsMap() {
        return Parameterized.isMap(keyType);
    }

}