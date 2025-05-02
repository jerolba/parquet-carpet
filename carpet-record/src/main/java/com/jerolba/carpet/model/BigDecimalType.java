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

import java.math.BigDecimal;
import java.math.RoundingMode;

public record BigDecimalType(boolean isNotNull, Integer precision, Integer scale, RoundingMode roundingMode)
        implements FieldType {

    public BigDecimalType {
        if (precision != null && scale != null) {
            if (precision <= 0) {
                throw new IllegalArgumentException("precision must be greater than 0");
            }
            if (scale < 0) {
                throw new IllegalArgumentException("scale must be zero or a positive value");
            }
            if (scale > precision) {
                throw new IllegalArgumentException("scale must be less than or equal to the precision");
            }
        } else if (precision != null) {
            throw new IllegalArgumentException("scale must be specified if precision is specified");
        } else if (scale != null) {
            throw new IllegalArgumentException("precision must be specified if scale is specified");
        }
    }

    public BigDecimalType notNull() {
        return new BigDecimalType(true, precision, scale, roundingMode);
    }

    public BigDecimalType withPrecisionScale(int precision, int scale) {
        return new BigDecimalType(isNotNull, precision, scale, roundingMode);
    }

    public BigDecimalType withRoundingMode(RoundingMode roundingMode) {
        return new BigDecimalType(isNotNull, precision, scale, roundingMode);
    }

    @Override
    public Class<BigDecimal> getClassType() {
        return BigDecimal.class;
    }

}
