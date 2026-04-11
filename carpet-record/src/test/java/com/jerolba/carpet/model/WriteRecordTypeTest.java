/**
 * Copyright 2026 Jerónimo López Bezanilla
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
package com.jerolba.carpet.model;

import static com.jerolba.carpet.model.FieldTypes.INTEGER;
import static com.jerolba.carpet.model.FieldTypes.STRING;
import static com.jerolba.carpet.model.WriteRecordModelType.writeRecordModel;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.function.Function;
import java.util.function.ToIntFunction;

import org.junit.jupiter.api.Test;

class WriteRecordTypeTest {

    record RootRecord(String id, Integer value) {
    }

    record RecordWithPrimitives(String id, int value, int key) {
    }

    @Test
    void fieldNameCanNotBeRepeated() {
        assertThrows(IllegalArgumentException.class, () -> {
            writeRecordModel(RootRecord.class)
                    .withField("id", STRING, RootRecord::id)
                    .withField("id", INTEGER, RootRecord::value);
        });
    }

    @Test
    void fieldTypeConsistencyIsNotVerified() {
        assertDoesNotThrow(() -> {
            writeRecordModel(RootRecord.class)
                    .withField("id", STRING, RootRecord::id)
                    .withField("value", STRING, RootRecord::value);
        });
    }

    @Test
    void supportsPrimitiveFields() {
        assertDoesNotThrow(() -> {
            writeRecordModel(RecordWithPrimitives.class)
                    .withField("id", STRING, RecordWithPrimitives::id)
                    .withField("value", RecordWithPrimitives::value)
                    .withPrimitiveField("key", INTEGER.notNull(),
                            (ToIntFunction<RecordWithPrimitives>) RecordWithPrimitives::key);
        });
    }

    @Test
    void withPrimitiveFieldValidatesProvidedType() {
        assertThrows(IllegalArgumentException.class, () -> {
            writeRecordModel(RecordWithPrimitives.class)
                    .withField("id", STRING, RecordWithPrimitives::id)
                    .withField("value", RecordWithPrimitives::value)
                    .withPrimitiveField("key", STRING.notNull(),
                            (ToIntFunction<RecordWithPrimitives>) RecordWithPrimitives::key);
        });
    }

    @Test
    void withPrimitiveFieldsMethodChecksProvidedFunction() {
        assertThrows(IllegalArgumentException.class, () -> {
            writeRecordModel(RecordWithPrimitives.class)
                    .withField("id", STRING, RecordWithPrimitives::id)
                    .withField("value", RecordWithPrimitives::value)
                    .withPrimitiveField("key", INTEGER.notNull(),
                            (Function<RecordWithPrimitives, Integer>) RecordWithPrimitives::key);
        });
    }

}
