package com.jerolba.carpet.impl.write;

import java.lang.reflect.RecordComponent;

public record RecordField(Class<?> targetClass, RecordComponent recordComponent, String fieldName, int idx) {

}
