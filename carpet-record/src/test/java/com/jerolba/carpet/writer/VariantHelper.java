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
package com.jerolba.carpet.writer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.parquet.variant.Variant;
import org.apache.parquet.variant.Variant.ObjectField;
import org.apache.parquet.variant.Variant.Type;
import org.apache.parquet.variant.VariantBuilder;
import org.apache.parquet.variant.VariantObjectBuilder;

public class VariantHelper {

    public static final Variant givenVariantWithNestedFields() {
        VariantBuilder builder = new VariantBuilder();
        VariantObjectBuilder object = builder.startObject();
        object.appendKey("a");
        object.appendString("some_value");
        object.appendKey("b");
        object.appendInt(42);
        builder.endObject();
        return builder.build();
    }

    public static final Variant givenVariantWithSingleValue() {
        VariantBuilder builder = new VariantBuilder();
        builder.appendString("foo");
        return builder.build();
    }

    public static void assertMatchesVariantWithNestedFields(Variant readVariant) {
        assertEquals(Type.OBJECT, readVariant.getType());
        assertEquals(2, readVariant.numObjectElements());
        ObjectField f0 = readVariant.getFieldAtIndex(0);
        assertEquals("a", f0.key);
        assertEquals(Type.STRING, f0.value.getType());
        assertEquals("some_value", f0.value.getString());
        ObjectField f1 = readVariant.getFieldAtIndex(1);
        assertEquals("b", f1.key);
        assertEquals(Type.INT, f1.value.getType());
        assertEquals(42, f1.value.getInt());
    }

    public static void assertMatchesVariantWithSingleValue(Variant readVariant) {
        assertEquals(Type.STRING, readVariant.getType());
        assertEquals("foo", readVariant.getString());
    }
}
