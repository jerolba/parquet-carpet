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

import static org.apache.parquet.schema.LogicalTypeAnnotation.variantType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.apache.parquet.schema.Types.buildGroup;
import static org.apache.parquet.schema.Types.primitive;

import java.util.function.BiConsumer;

import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.variant.Variant;
import org.apache.parquet.variant.VariantValueWriter;

class VariantWriter {

    private static final GroupType SIMPLE_VARIANT_SCHEMA = buildGroup(OPTIONAL)
            .as(variantType((byte) 1))
            .addField(primitive(BINARY, REQUIRED).named("metadata"))
            .addField(primitive(BINARY, REQUIRED).named("value"))
            .named("variant");

    public static BiConsumer<RecordConsumer, Object> simpleVariantWriter() {
        return (consumer, v) -> VariantValueWriter.write(consumer, SIMPLE_VARIANT_SCHEMA, (Variant) v);
    }

}
