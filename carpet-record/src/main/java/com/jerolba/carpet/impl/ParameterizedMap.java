package com.jerolba.carpet.impl;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class ParameterizedMap {

    private final Type keyType;
    private final Type valueType;

    public ParameterizedMap(ParameterizedType type) {
        this.keyType = type.getActualTypeArguments()[0];
        this.valueType = type.getActualTypeArguments()[1];
    }

    public Class<?> getValueActualType() {
        return Parameterized.getClassFromType(valueType, "in Map value");
    }

    public Class<?> getKeyActualType() {
        return Parameterized.getClassFromType(keyType, "in Map key");
    }

    public ParameterizedMap getValueTypeAsMap() {
        if (valueType instanceof ParameterizedType paramType) {
            return new ParameterizedMap(paramType);
        }
        return null;
    }

    public ParameterizedCollection getValueTypeAsCollection() {
        if (valueType instanceof ParameterizedType paramType) {
            return new ParameterizedCollection(paramType);
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