package com.jerolba.carpet.impl.read.converter;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;

import com.jerolba.carpet.impl.read.ReadReflection.ConstructorParams;

public class StringConverter extends PrimitiveConverter {

    private String[] dict = null;
    private final ConstructorParams constructor;
    private final int idx;

    public StringConverter(ConstructorParams constructor, int idx) {
        this.constructor = constructor;
        this.idx = idx;
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
        dict = new String[dictionary.getMaxId() + 1];
        for (int i = 0; i <= dictionary.getMaxId(); i++) {
            dict[i] = convert(dictionary.decodeToBinary(i));
        }
    }

    @Override
    public void addValueFromDictionary(int dictionaryId) {
        constructor.c[idx] = dict[dictionaryId];
    }

    private String convert(Binary value) {
        return value.toStringUsingUTF8();
    }
}