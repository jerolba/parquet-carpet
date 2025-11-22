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

import java.lang.reflect.RecordComponent;
import java.util.HashSet;
import java.util.Set;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.annotation.FieldId;

class FieldIdMapper {

    private final Set<Integer> usedIds = new HashSet<>();

    /**
     * Gets the field ID from a record component if it has the @FieldId annotation.
     *
     * @param recordComponent the record component to check
     * @return the field ID if present, or null if no @FieldId annotation is present
     */
    Integer getFieldId(RecordComponent recordComponent) {
        FieldId annotation = recordComponent.getAnnotation(FieldId.class);
        if (annotation == null) {
            return null;
        }
        int fieldId = annotation.value();
        if (usedIds.contains(fieldId)) {
            throw new RecordTypeConversionException(
                    "Duplicate field ID " + fieldId + " found in record "
                            + recordComponent.getDeclaringRecord().getSimpleName() +
                            ". Field IDs must be unique within the same record scope.");
        }
        usedIds.add(fieldId);
        return fieldId;
    }

}