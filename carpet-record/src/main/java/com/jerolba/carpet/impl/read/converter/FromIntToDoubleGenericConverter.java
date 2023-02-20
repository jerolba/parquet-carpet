package com.jerolba.carpet.impl.read.converter;

import java.util.function.Consumer;

import org.apache.parquet.io.api.PrimitiveConverter;

public class FromIntToDoubleGenericConverter extends PrimitiveConverter {

    private final Consumer<Object> listConsumer;

    public FromIntToDoubleGenericConverter(Consumer<Object> listConsumer) {
        this.listConsumer = listConsumer;
    }

    @Override
    public void addLong(long value) {
        listConsumer.accept((double) value);
    }

    @Override
    public void addInt(int value) {
        listConsumer.accept((double) value);
    }

}
