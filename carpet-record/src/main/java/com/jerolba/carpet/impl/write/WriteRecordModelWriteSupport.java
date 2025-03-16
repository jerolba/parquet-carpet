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

import com.jerolba.carpet.model.WriteRecordModelType;

class WriteRecordModelWriteSupport<T> extends WriteSupport<T> {

    private final WriteRecordModelType<T> rootWriteRecordModel;
    private final Map<String, String> extraMetaData;
    private final CarpetWriteConfiguration carpetConfiguration;
    private MessageWriter<T> messageWriter;

    WriteRecordModelWriteSupport(WriteRecordModelType<T> rootWriteRecordModel, Map<String, String> extraMetaData,
            CarpetWriteConfiguration carpetConfiguration) {
        this.rootWriteRecordModel = rootWriteRecordModel;
        this.extraMetaData = extraMetaData;
        this.carpetConfiguration = carpetConfiguration;
    }

    @Override
    public String getName() {
        return rootWriteRecordModel.getClassType().getName();
    }

    @Override
    public WriteContext init(Configuration configuration) {
        return initContext();
    }

    @Override
    public WriteContext init(ParquetConfiguration configuration) {
        return initContext();
    }

    private WriteContext initContext() {
        WriteRecordModel2Schema modelRecord2Schema = new WriteRecordModel2Schema(carpetConfiguration);
        MessageType schema = modelRecord2Schema.createSchema(rootWriteRecordModel);
        return new WriteContext(schema, this.extraMetaData);
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        try {
            messageWriter = new MessageWriter<>(recordConsumer, rootWriteRecordModel, carpetConfiguration);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(T record) {
        messageWriter.write(record);
    }

    private static class MessageWriter<T> {

        private final RecordConsumer recordConsumer;
        private final WriteRecordModelWriter writer;

        MessageWriter(RecordConsumer recordConsumer, WriteRecordModelType<T> rootWriteRecordModel,
                CarpetWriteConfiguration carpetConfiguration) {
            this.recordConsumer = recordConsumer;
            this.writer = new WriteRecordModelWriter(recordConsumer, rootWriteRecordModel, carpetConfiguration);
        }

        void write(T record) {
            recordConsumer.startMessage();
            writer.write(record);
            recordConsumer.endMessage();
        }

    }

}
