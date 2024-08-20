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
package com.jerolba.carpet.impl.read;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

public class CarpetReadSupport<T> extends ReadSupport<T> {

    private final Class<T> readClass;
    private final CarpetReadConfiguration carpetConfiguration;
    private final ColumnToFieldMapper columnToFieldMapper;

    public CarpetReadSupport(Class<T> readClass, CarpetReadConfiguration carpetConfiguration) {
        this.readClass = readClass;
        this.carpetConfiguration = carpetConfiguration;
        this.columnToFieldMapper = new ColumnToFieldMapper(carpetConfiguration.fieldMatchingStrategy());
    }

    @Override
    public RecordMaterializer<T> prepareForRead(Configuration configuration,
            Map<String, String> keyValueMetaData, MessageType fileSchema, ReadContext readContext) {
        return new CarpetMaterializer<>(readClass, readContext.getRequestedSchema(), columnToFieldMapper);
    }

    @Override
    public ReadContext init(InitContext initContext) {
        var validation = new SchemaValidation(carpetConfiguration.isFailOnMissingColumn(),
                carpetConfiguration.isFailNarrowingPrimitiveConversion(),
                carpetConfiguration.isFailOnNullForPrimitives());

        SchemaFilter schemaFilter = new SchemaFilter(validation, columnToFieldMapper);
        MessageType projection = schemaFilter.project(readClass, initContext.getFileSchema());
        Map<String, String> metadata = new LinkedHashMap<>();
        return new ReadContext(projection, metadata);
    }

}