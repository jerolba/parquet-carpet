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

import java.math.RoundingMode;

public class DecimalConfig {

    private final Integer precision;
    private final Integer scale;
    private final RoundingMode roundingMode;

    private DecimalConfig(Integer precision, Integer scale, RoundingMode roundingMode) {
        this.precision = precision;
        this.scale = scale;
        this.roundingMode = roundingMode;
    }

    public Integer precision() {
        return precision;
    }

    public Integer scale() {
        return scale;
    }

    public RoundingMode roundingMode() {
        return roundingMode;
    }

    public static DecimalConfig decimalConfig() {
        return new DecimalConfig(null, null, null);
    }

    public DecimalConfig withPrecionAndScale(int precision, int scale) {
        if (precision <= 0) {
            throw new IllegalArgumentException("precision must be greater than 0");
        }
        if (scale < 0) {
            throw new IllegalArgumentException("scale must be zero or a positive value");
        }
        if (scale > precision) {
            throw new IllegalArgumentException("scale must be less than or equal to the precision");
        }
        return new DecimalConfig(precision, scale, roundingMode);
    }

    public DecimalConfig withRoundingMode(RoundingMode roundingMode) {
        return new DecimalConfig(precision, scale, roundingMode);
    }

    public boolean arePrecisionAndScaleConfigured() {
        return precision != null && scale != null;
    }

}
