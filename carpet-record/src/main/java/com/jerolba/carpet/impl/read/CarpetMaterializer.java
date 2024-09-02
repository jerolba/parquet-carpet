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

import java.util.Map;

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

class CarpetMaterializer<T> extends RecordMaterializer<T> {

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