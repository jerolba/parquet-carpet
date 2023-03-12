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

import static com.jerolba.carpet.impl.read.CarpetListIntermediateConverter.createCollectionConverter;

import java.util.function.Consumer;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

import com.jerolba.carpet.impl.ParameterizedCollection;

class CarpetListConverter extends GroupConverter {

    private final Consumer<Object> groupConsumer;
    private final Converter converter;
    private final ListHolder listHolder = new ListHolder();

    CarpetListConverter(GroupType requestedSchema, ParameterizedCollection parameterized,
            Consumer<Object> groupConsumer) {
        this.groupConsumer = groupConsumer;

        Type listChild = requestedSchema.getFields().get(0);
        boolean threeLevel = SchemaValidation.isThreeLevel(listChild);
        if (threeLevel) {
            converter = new CarpetListIntermediateConverter(listChild, parameterized, listHolder);
        } else {
            converter = createCollectionConverter(listChild, parameterized, listHolder::add);
        }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return converter;
    }

    @Override
    public void start() {
        listHolder.start();
    }

    @Override
    public void end() {
        groupConsumer.accept(listHolder.end());
    }

}