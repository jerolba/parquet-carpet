package com.jerolba.carpet.impl.read.converter;

import java.util.function.Consumer;

import org.apache.parquet.io.api.PrimitiveConverter;

public class FromIntToLongGenericConverter extends PrimitiveConverter {

    private final Consumer<Object> listConsumer;

    public FromIntToLongGenericConverter(Consumer<Object> listConsumer) {
        this.listConsumer = listConsumer;
    }

    @Override
    public void addInt(int value) {
        listConsumer.accept((long) value);
    }

    @Override
    public void addLong(long value) {
        listConsumer.accept(value);
    }

}
