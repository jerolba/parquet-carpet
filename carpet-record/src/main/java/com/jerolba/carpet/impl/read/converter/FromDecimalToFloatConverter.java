package com.jerolba.carpet.impl.read.converter;

import org.apache.parquet.io.api.PrimitiveConverter;

import com.jerolba.carpet.impl.read.ReadReflection.ConstructorParams;

public class FromDecimalToFloatConverter extends PrimitiveConverter {

    private final ConstructorParams constructor;
    private final int idx;

    public FromDecimalToFloatConverter(ConstructorParams constructor, int idx) {
        this.constructor = constructor;
        this.idx = idx;
    }

    @Override
    public void addDouble(double value) {
        constructor.c[idx] = (float) value;
    }

    @Override
    public void addFloat(float value) {
        constructor.c[idx] = value;
    }

}
