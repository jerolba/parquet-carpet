package com.jerolba.carpet.impl.write;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

public class CarpetWriterSupport<T> extends WriteSupport<T> {

    private final Class<T> recordClass;
    private final Map<String, String> extraMetaData;
    private final CarpetWriteConfiguration carpetConfiguration;
    private CarpetMessageWriter<T> carpetWriter;

    public CarpetWriterSupport(Class<T> recordClass, Map<String, String> extraMetaData,
            CarpetWriteConfiguration carpetConfiguration) {
        this.recordClass = recordClass;
        this.extraMetaData = extraMetaData;
        this.carpetConfiguration = carpetConfiguration;
    }

    @Override
    public String getName() {
        return recordClass.getName();
    }

    @Override
    public WriteContext init(Configuration configuration) {
        JavaRecord2Schema javaRecord2Schema = new JavaRecord2Schema(carpetConfiguration);
        MessageType schema = javaRecord2Schema.createSchema(recordClass);
        return new WriteContext(schema, this.extraMetaData);
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        try {
            carpetWriter = new CarpetMessageWriter<>(recordConsumer, recordClass, carpetConfiguration);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(T record) {
        carpetWriter.write(record);
    }
}