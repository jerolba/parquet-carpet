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

public final class BinaryAliasedType extends BinaryType {

    private final BinaryLogicalType logicalType;

    BinaryAliasedType(boolean isNotNull, Integer fieldId, BinaryLogicalType logicalType) {
        super(isNotNull, fieldId);
        this.logicalType = logicalType;
    }

    @Override
    public BinaryAliasedType notNull() {
        return new BinaryAliasedType(true, fieldId(), logicalType);
    }

    @Override
    public BinaryAliasedType fieldId(Integer fieldId) {
        return new BinaryAliasedType(isNotNull(), fieldId, logicalType);
    }

    public BinaryLogicalType logicalType() {
        return logicalType;
    }

}
