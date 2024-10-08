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

import com.jerolba.carpet.FieldMatchingStrategy;

public class CarpetReadConfiguration {

    private final boolean failOnMissingColumn;
    private final boolean failNarrowingPrimitiveConversion;
    private final boolean failOnNullForPrimitives;
    private final FieldMatchingStrategy fieldMatchingStrategy;

    public CarpetReadConfiguration(boolean failOnMissingColumn,
            boolean failNarrowingPrimitiveConversion,
            boolean failOnNullForPrimitives,
            FieldMatchingStrategy fieldMatchingStrategy) {
        this.failOnMissingColumn = failOnMissingColumn;
        this.failNarrowingPrimitiveConversion = failNarrowingPrimitiveConversion;
        this.failOnNullForPrimitives = failOnNullForPrimitives;
        this.fieldMatchingStrategy = fieldMatchingStrategy;
    }

    public boolean isFailOnMissingColumn() {
        return failOnMissingColumn;
    }

    public boolean isFailNarrowingPrimitiveConversion() {
        return failNarrowingPrimitiveConversion;
    }

    public boolean isFailOnNullForPrimitives() {
        return failOnNullForPrimitives;
    }

    public FieldMatchingStrategy fieldMatchingStrategy() {
        return fieldMatchingStrategy;
    }

}
