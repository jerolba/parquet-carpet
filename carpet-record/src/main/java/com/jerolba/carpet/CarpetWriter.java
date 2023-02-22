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
package com.jerolba.carpet;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import com.jerolba.carpet.impl.JavaRecord2Schema;

public class CarpetWriter<T> {

    public static <T> Builder<T> builder(OutputFile file, Class<T> recordClass) {
        return new Builder<>(file, recordClass);
    }

    public static class Builder<T> extends ParquetWriter.Builder<T, Builder<T>> {

        private final Class<T> recordClass;
        private Map<String, String> extraMetaData = new HashMap<>();
        private AnnotatedLevels annotatedLevels = AnnotatedLevels.THREE;

        private Builder(OutputFile file, Class<T> recordClass) {
            super(file);
            this.recordClass = recordClass;
        }

        public Builder<T> withExtraMetaData(Map<String, String> extraMetaData) {
            this.extraMetaData = extraMetaData;
            return this;
        }

        @Override
        protected Builder<T> self() {
            return this;
        }

        public Builder<T> levelStructure(AnnotatedLevels annotatedLevels) {
            this.annotatedLevels = annotatedLevels;
            return self();
        }

        @Override
        protected WriteSupport<T> getWriteSupport(Configuration conf) {
            CarpetWriteConfiguration carpetCfg = new CarpetWriteConfiguration(annotatedLevels);
            return new CarpetWriterSupport<>(recordClass, extraMetaData, carpetCfg);
        }

    }

    private static class CarpetWriterSupport<T> extends WriteSupport<T> {

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

}
