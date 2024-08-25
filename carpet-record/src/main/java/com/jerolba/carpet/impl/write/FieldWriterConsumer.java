package com.jerolba.carpet.impl.write;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.parquet.io.api.RecordConsumer;

class FieldWriterConsumer implements Consumer<Object> {

    private final RecordConsumer recordConsumer;
    private final String fieldName;
    private final int idx;
    private final Function<Object, Object> accessor;
    private final BiConsumer<RecordConsumer, Object> writer;

    public FieldWriterConsumer(RecordConsumer recordConsumer, RecordField recordField,
            BiConsumer<RecordConsumer, Object> writer) {
        this.recordConsumer = recordConsumer;
        this.fieldName = recordField.fieldName();
        this.idx = recordField.idx();
        this.accessor = recordField.getAccessor();
        this.writer = writer;
    }

    @Override
    public void accept(Object object) {
        var value = accessor.apply(object);
        if (value != null) {
            recordConsumer.startField(fieldName, idx);
            writer.accept(recordConsumer, value);
            recordConsumer.endField(fieldName, idx);
        }
    }

}