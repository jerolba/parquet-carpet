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
package com.jerolba.carpet.model;

import java.util.Set;

public record StringType(boolean isNotNull, BinaryLogicalType logicalType) implements FieldType {

    private static final Set<BinaryLogicalType> VALID_LOGICAL_TYPES = Set.of(
            BinaryLogicalType.STRING,
            BinaryLogicalType.ENUM,
            BinaryLogicalType.JSON);

    public StringType {
        if (logicalType != null && !VALID_LOGICAL_TYPES.contains(logicalType)) {
            throw new IllegalArgumentException("Invalid logical type for StringType: " + logicalType);
        }
    }

    public StringType notNull() {
        return new StringType(true, logicalType);
    }

    public StringType asJson() {
        return new StringType(isNotNull, BinaryLogicalType.JSON);
    }

    public StringType asEnum() {
        return new StringType(isNotNull, BinaryLogicalType.ENUM);
    }

    @Override
    public Class<String> getClassType() {
        return String.class;
    }

}