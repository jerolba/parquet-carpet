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