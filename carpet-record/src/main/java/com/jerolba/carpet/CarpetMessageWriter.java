package com.jerolba.carpet;

import org.apache.parquet.io.api.RecordConsumer;

import com.jerolba.carpet.impl.write.CarpetRecordWriter;

class CarpetMessageWriter<T> {

    private final RecordConsumer recordConsumer;
    private final CarpetRecordWriter writer;

    CarpetMessageWriter(RecordConsumer recordConsumer, Class<T> recordClass,
            CarpetWriteConfiguration carpetConfiguration) {
        this.recordConsumer = recordConsumer;
        this.writer = new CarpetRecordWriter(recordConsumer, recordClass, carpetConfiguration);
    }

    void write(T record) {
        recordConsumer.startMessage();
        writer.write(record);
        recordConsumer.endMessage();
    }

}