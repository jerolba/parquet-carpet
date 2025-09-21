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

import java.nio.ByteBuffer;
import java.util.function.Consumer;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.variant.ImmutableMetadata;
import org.apache.parquet.variant.Variant;
import org.apache.parquet.variant.VariantBuilder;
import org.apache.parquet.variant.VariantConverters;
import org.apache.parquet.variant.VariantConverters.ParentConverter;

public class VariantConverter extends GroupConverter implements ParentConverter<VariantBuilder> {

    private final Consumer<Object> consumer;
    private final GroupConverter wrappedConverter;

    private VariantBuilder builder = null;
    private ImmutableMetadata metadata = null;

    public VariantConverter(GroupType asGroupType, Consumer<Object> consumer) {
        this.consumer = consumer;
        this.wrappedConverter = VariantConverters.newVariantConverter(asGroupType, this::setMetadata, this);
    }

    @Override
    public boolean isPrimitive() {
        return false;
    }

    private void setMetadata(ByteBuffer metadataBuffer) {
        if (metadata == null || metadata.getEncodedBuffer() != metadataBuffer) {
            this.metadata = new ImmutableMetadata(metadataBuffer);
        }

        this.builder = new VariantBuilder(metadata);
    }

    @Override
    public void build(Consumer<VariantBuilder> buildConsumer) {
        buildConsumer.accept(builder);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return wrappedConverter.getConverter(fieldIndex);
    }

    @Override
    public void start() {
        wrappedConverter.start();
    }

    @Override
    public void end() {
        wrappedConverter.end();

        builder.appendNullIfEmpty();
        Variant variant = builder.build();
        consumer.accept(variant);
        this.builder = null;
    }
}
