package com.jerolba.carpet.impl;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class ParameterizedCollection {

    private final Type collectionType;

    public ParameterizedCollection(ParameterizedType type) {
        this.collectionType = type.getActualTypeArguments()[0];
    }

    public Class<?> getActualType() {
        return Parameterized.getClassFromType(collectionType, "in Collection");
    }

    public ParameterizedCollection getParametizedAsCollection() {
        if (collectionType instanceof ParameterizedType paramType) {
            return new ParameterizedCollection(paramType);
        }
        return null;
    }

    public ParameterizedMap getParametizedAsMap() {
        if (collectionType instanceof ParameterizedType paramType) {
            return new ParameterizedMap(paramType);
        }
        return null;
    }

    public boolean isCollection() {
        return Parameterized.isCollection(collectionType);
    }

    public boolean isMap() {
        return Parameterized.isMap(collectionType);
    }

}