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

public class EnumTypeBuilder {

    private final boolean notNull;
    private final BinaryLogicalType logicalType;

    EnumTypeBuilder() {
        this(false, null);
    }

    private EnumTypeBuilder(boolean notNull, BinaryLogicalType logicalType) {
        this.notNull = notNull;
        this.logicalType = logicalType;
    }

    public EnumTypeBuilder notNull() {
        return new EnumTypeBuilder(true, logicalType);
    }

    public EnumTypeBuilder asString() {
        return new EnumTypeBuilder(notNull, BinaryLogicalType.STRING);
    }

    public EnumType ofType(Class<? extends Enum<?>> enumClass) {
        return new EnumType(notNull, enumClass, logicalType);
    }

}