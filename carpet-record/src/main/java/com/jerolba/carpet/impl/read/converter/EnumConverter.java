package com.jerolba.carpet.impl.read.converter;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;

import com.jerolba.carpet.impl.read.ReadReflection.ConstructorParams;

public class EnumConverter extends PrimitiveConverter {

    private Enum<?>[] dict = null;
    private final ConstructorParams constructor;
    private final int idx;
    private final Class<? extends Enum> asEnum;

    public EnumConverter(ConstructorParams constructor, int idx, Class<?> type) {
        this.constructor = constructor;
        this.idx = idx;
        this.asEnum = type.asSubclass(Enum.class);
    }

    @Override
    public void addBinary(Binary value) {
        constructor.c[idx] = convert(value);
    }

    @Override
    public boolean hasDictionarySupport() {
        return true;
    }

    @Override
    public void setDictionary(Dictionary dictionary) {
        dict = new Enum[dictionary.getMaxId() + 1];
        for (int i = 0; i <= dictionary.getMaxId(); i++) {
            dict[i] = convert(dictionary.decodeToBinary(i));
        }
    }

    @Override
    public void addValueFromDictionary(int dictionaryId) {
        constructor.c[idx] = dict[dictionaryId];
    }

    private Enum<?> convert(Binary value) {
        String str = value.toStringUsingUTF8();
        return Enum.valueOf(asEnum, str);
    }
}