package com.jerolba.carpet.impl.write;

import java.util.function.Consumer;
import java.util.function.Function;

public abstract class FieldWriter implements Consumer<Object> {

    protected final RecordField recordField;
    protected final Function<Object, Object> accesor;

    public FieldWriter(RecordField recordField) {
        this.recordField = recordField;
        this.accesor = Reflection.recordAccessor(recordField.targetClass(), recordField.recordComponent());
    }

    public abstract void writeField(Object object);

    @Override
    public void accept(Object object) {
        writeField(object);
    }

}
