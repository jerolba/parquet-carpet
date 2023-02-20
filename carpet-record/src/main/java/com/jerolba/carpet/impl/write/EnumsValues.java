package com.jerolba.carpet.impl.write;

import org.apache.parquet.io.api.Binary;

class EnumsValues {

    private final Binary[] values;

    EnumsValues(Class<?> enumType) {
        Object[] enums = enumType.getEnumConstants();
        values = new Binary[enums.length];
        for (int i = 0; i < enums.length; i++) {
            values[i] = Binary.fromString(((Enum<?>) enums[i]).name());
        }
    }

    public Binary getValue(Object v) {
        return values[((Enum<?>) v).ordinal()];
    }

}