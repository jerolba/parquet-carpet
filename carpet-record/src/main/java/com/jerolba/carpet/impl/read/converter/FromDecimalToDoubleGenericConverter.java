package com.jerolba.carpet.impl.read.converter;

import java.util.function.Consumer;

import org.apache.parquet.io.api.PrimitiveConverter;

public class FromDecimalToDoubleGenericConverter extends PrimitiveConverter {

    private final Consumer<Object> listConsumer;

    public FromDecimalToDoubleGenericConverter(Consumer<Object> listConsumer) {
        this.listConsumer = listConsumer;
    }

    @Override
    public void addDouble(double value) {
        listConsumer.accept(value);
    }

    @Override
    public void addFloat(float value) {
        listConsumer.accept((double) value);
    }

}
