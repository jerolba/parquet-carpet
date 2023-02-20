package com.jerolba.carpet.impl.read.converter;

import org.apache.parquet.io.api.PrimitiveConverter;

import com.jerolba.carpet.impl.read.ReadReflection.ConstructorParams;

public class BooleanConverter extends PrimitiveConverter {

    private final ConstructorParams constructor;
    private final int idx;

    public BooleanConverter(ConstructorParams constructor, int idx) {
        this.constructor = constructor;
        this.idx = idx;
    }

    @Override
    public void addBoolean(boolean value) {
        constructor.c[idx] = value;
    }

}
