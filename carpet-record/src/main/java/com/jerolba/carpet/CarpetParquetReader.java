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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import com.jerolba.carpet.impl.read.CarpetGroupConverter;
import com.jerolba.carpet.impl.read.SchemaFilter;
import com.jerolba.carpet.impl.read.SchemaValidation;

public class CarpetParquetReader {

    public static <T> Builder<T> builder(InputFile file, Class<T> readClass) {
        return new Builder<>(file, readClass);
    }

    public static class Builder<T> extends ParquetReader.Builder<T> {

        private final Class<T> readClass;
        private boolean failOnMissingColumn = true;
        private boolean strictNumericType = false;
        private boolean failOnNullForPrimitives = false;

        private Builder(InputFile file, Class<T> readClass) {
            super(file);
            this.readClass = readClass;
        }

        /**
         * Feature that determines whether encountering of missed parquet column should
         * result in a failure (by throwing a RecordTypeConversionException) or not.
         *
         * Feature is enabled by default.
         *
         * @param failOnMissingColumn
         * @return Carpet Reader Builder
         */
        public Builder<T> failOnMissingColumn(boolean failOnMissingColumn) {
            this.failOnMissingColumn = failOnMissingColumn;
            return this;
        }

        public Builder<T> strictNumericType(boolean strictNumericType) {
            this.strictNumericType = strictNumericType;
            return this;
        }

        /**
         * Feature that determines whether encountering null is an error when
         * deserializing into Java primitive types (like 'int' or 'double'). If it is, a
         * RecordTypeConversionException is thrown to indicate this; if not, default
         * value is used (0 for 'int', 0.0 for double, same defaulting as what JVM
         * uses).
         *
         * Feature is disabled by default.
         *
         * @param failOnNullForPrimitives
         * @return Carpet Reader Builder
         */
        public Builder<T> failOnNullForPrimitives(boolean failOnNullForPrimitives) {
            this.failOnNullForPrimitives = failOnNullForPrimitives;
            return this;
        }

        @Override
        protected ReadSupport<T> getReadSupport() {
            CarpetReadConfiguration configuration = new CarpetReadConfiguration(failOnMissingColumn,
                    strictNumericType,
                    failOnNullForPrimitives);
            CarpetReadSupport<T> readSupport = new CarpetReadSupport<>(readClass, configuration);
            return readSupport;
        }

    }

    public static class CarpetReadSupport<T> extends ReadSupport<T> {

        private final Class<T> readClass;
        private final CarpetReadConfiguration carpetConfiguration;

        public CarpetReadSupport(Class<T> readClass, CarpetReadConfiguration carpetConfiguration) {
            this.readClass = readClass;
            this.carpetConfiguration = carpetConfiguration;
        }

        @Override
        public RecordMaterializer<T> prepareForRead(Configuration configuration,
                Map<String, String> keyValueMetaData, MessageType fileSchema, ReadContext readContext) {
            return new CarpetMaterializer<>(readClass, readContext.getRequestedSchema());
        }

        @Override
        public ReadContext init(Configuration configuration,
                Map<String, String> keyValueMetaData,
                MessageType fileSchema) {

            var validation = new SchemaValidation(carpetConfiguration.isFailOnMissingColumn(),
                    carpetConfiguration.isStrictNumericType(),
                    carpetConfiguration.isFailOnNullForPrimitives());
            SchemaFilter projectedSchema = new SchemaFilter(validation, fileSchema);
            MessageType projection = projectedSchema.project(readClass);
            Map<String, String> metadata = new LinkedHashMap<>();
            return new ReadContext(projection, metadata);
        }

    }

    static class CarpetMaterializer<T> extends RecordMaterializer<T> {

        private final CarpetGroupConverter root;
        private T value;

        CarpetMaterializer(Class<T> readClass, MessageType requestedSchema) {
            this.root = new CarpetGroupConverter(requestedSchema, readClass, record -> this.value = (T) record);
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

}
