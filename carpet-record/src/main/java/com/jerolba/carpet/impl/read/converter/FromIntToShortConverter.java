package com.jerolba.carpet.impl.read.converter;

import org.apache.parquet.io.api.PrimitiveConverter;

import com.jerolba.carpet.impl.read.ReadReflection.ConstructorParams;

public class FromIntToShortConverter extends PrimitiveConverter {

    private final ConstructorParams constructor;
    private final int idx;

    public FromIntToShortConverter(ConstructorParams constructor, int idx) {
        this.constructor = constructor;
        this.idx = idx;
    }

    @Override
    public void addLong(long value) {
        constructor.c[idx] = (short) value;
    }

    @Override
    public void addInt(int value) {
        constructor.c[idx] = (short) value;
    }

}
