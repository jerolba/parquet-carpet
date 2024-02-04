package com.jerolba.carpet.impl.read;

import java.util.Map;

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

public class CarpetMaterializer<T> extends RecordMaterializer<T> {

    private final GroupConverter root;
    private T value;

    public CarpetMaterializer(Class<T> readClass, MessageType requestedSchema,
            ColumnToFieldMapper columnToFieldMapper) {
        if (Map.class.isAssignableFrom(readClass)) {
            this.root = new CarpetGroupAsMapConverter(readClass, requestedSchema, value -> this.value = (T) value);
        } else {
            this.root = new MainGroupConverter(columnToFieldMapper)
                    .newCarpetGroupConverter(requestedSchema, readClass, record -> this.value = (T) record);
        }
    }

    @Override
    public T getCurrentRecord() {
        return value;
    }

    @Override
    public GroupConverter getRootConverter() {
        return root;
    }

}