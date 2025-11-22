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

public record EnumType(boolean isNotNull, Integer fieldId, Class<? extends Enum<?>> enumClass, BinaryLogicalType logicalType)
        implements FieldType {

    private static final Set<BinaryLogicalType> VALID_LOGICAL_TYPES = Set.of(
            BinaryLogicalType.STRING,
            BinaryLogicalType.ENUM);

    public EnumType {
        if (logicalType != null && !VALID_LOGICAL_TYPES.contains(logicalType)) {
            throw new IllegalArgumentException("Invalid logical type for StringType: " + logicalType);
        }
    }

    public EnumType notNull() {
        return new EnumType(true, fieldId, enumClass, logicalType);
    }

    public EnumType fieldId(Integer fieldId) {
        return new EnumType(isNotNull, fieldId, enumClass, logicalType);
    }

    public EnumType asString() {
        return new EnumType(isNotNull, fieldId, enumClass, BinaryLogicalType.STRING);
    }

    @Override
    public Class<? extends Enum<?>> getClassType() {
        return enumClass;
    }

}