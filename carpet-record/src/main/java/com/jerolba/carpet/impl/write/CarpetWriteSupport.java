/**
 * Copyright 2023 Jerónimo López Bezanilla
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jerolba.carpet.impl.write;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

class CarpetWriteSupport<T> extends WriteSupport<T> {

    private final Class<T> recordClass;
    private final Map<String, String> extraMetaData;
    private final CarpetWriteConfiguration carpetConfiguration;
    private CarpetMessageWriter<T> carpetWriter;

    public CarpetWriteSupport(Class<T> recordClass, Map<String, String> extraMetaData,
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
    public WriteContext init(ParquetConfiguration configuration) {
        JavaRecord2Schema javaRecord2Schema = new JavaRecord2Schema(carpetConfiguration);
        MessageType schema = javaRecord2Schema.createSchema(recordClass);
        return new WriteContext(schema, this.extraMetaData);
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

    private static class CarpetMessageWriter<T> {

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

}