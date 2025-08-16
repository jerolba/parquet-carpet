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

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.parquet.io.api.RecordConsumer;

class PrimitiveFieldWriter implements Consumer<Object> {

    private final RecordConsumer recordConsumer;
    private final String parquetFieldName;
    private final int idx;
    private final BiConsumer<RecordConsumer, Object> writer;

    public PrimitiveFieldWriter(RecordConsumer recordConsumer, String parquetFieldName, int idx,
            BiConsumer<RecordConsumer, Object> writer) {
        this.recordConsumer = recordConsumer;
        this.parquetFieldName = parquetFieldName;
        this.idx = idx;
        this.writer = writer;
    }

    @Override
    public void accept(Object object) {
        recordConsumer.startField(parquetFieldName, idx);
        writer.accept(recordConsumer, object);
        recordConsumer.endField(parquetFieldName, idx);
    }

}