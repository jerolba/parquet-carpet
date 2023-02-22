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

import static com.jerolba.carpet.impl.write.SimpleCollectionItemConsumerFactory.buildSimpleElementConsumer;

import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.parquet.io.api.RecordConsumer;

import com.jerolba.carpet.CarpetWriteConfiguration;
import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.impl.ParameterizedCollection;
import com.jerolba.carpet.impl.ParameterizedMap;

public class OneLevelStructureWriter {

    private final RecordConsumer recordConsumer;
    private final CarpetWriteConfiguration carpetConfiguration;

    public OneLevelStructureWriter(RecordConsumer recordConsumer, CarpetWriteConfiguration carpetConfiguration) {
        this.recordConsumer = recordConsumer;
        this.carpetConfiguration = carpetConfiguration;
    }

    public Consumer<Object> createCollectionWriter(ParameterizedCollection parametized, RecordField field) {
        if (parametized.isCollection()) {
            throw new RecordTypeConversionException(
                    "Nested collection in a collection is not supported in single level structure codification");
        }
        BiConsumer<RecordConsumer, Object> elemConsumer = null;
        if (parametized.isMap()) {
            ParameterizedMap parametizedChild = parametized.getParametizedAsMap();
            var mapStructWriter = new MapStructureWriter(recordConsumer, carpetConfiguration);
            Consumer<Object> childWriter = mapStructWriter.createMapWriter(parametizedChild, null);
            elemConsumer = (consumer, v) -> childWriter.accept(v);
        } else {
            Class<?> type = parametized.getActualType();
            elemConsumer = buildSimpleElementConsumer(type, recordConsumer, carpetConfiguration);
        }
        if (elemConsumer == null) {
            throw new RecordTypeConversionException("Unsuported type in collection");
        }
        return new OneLevelCollectionFieldWriter(field, elemConsumer);
    }

    private class OneLevelCollectionFieldWriter extends FieldWriter {

        private final BiConsumer<RecordConsumer, Object> consumer;

        public OneLevelCollectionFieldWriter(RecordField recordField, BiConsumer<RecordConsumer, Object> consumer) {
            super(recordField);
            this.consumer = consumer;
        }

        @Override
        public void writeField(Object object) {
            var value = accesor.apply(object);
            if (value != null) {
                recordConsumer.startField(recordField.fieldName(), recordField.idx());
                Collection<?> coll = (Collection<?>) value;
                for (var v : coll) {
                    consumer.accept(recordConsumer, v);
                }
                recordConsumer.endField(recordField.fieldName(), recordField.idx());
            }
        }
    }

}
