package com.jerolba.carpet.impl.read.converter;

import java.util.function.Consumer;

import org.apache.parquet.io.api.PrimitiveConverter;

public class FromIntToShortGenericConverter extends PrimitiveConverter {

    private final Consumer<Object> listConsumer;

    public FromIntToShortGenericConverter(Consumer<Object> listConsumer) {
        this.listConsumer = listConsumer;
    }

    @Override
    public void addInt(int value) {
        listConsumer.accept((short) value);
    }

    @Override
    public void addLong(long value) {
        listConsumer.accept((short) value);
    }

}
