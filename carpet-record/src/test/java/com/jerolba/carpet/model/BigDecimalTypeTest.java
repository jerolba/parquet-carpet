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

import static com.jerolba.carpet.model.FieldTypes.BIG_DECIMAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class BigDecimalTypeTest {

    @Test
    void validPrecisionAndScale() {
        BigDecimalType type = new BigDecimalType(true, null, 10, 2, null);
        assertEquals(10, type.precision());
        assertEquals(2, type.scale());
    }

    @Test
    void validPrecisionAndScaleUsingBuilder() {
        BigDecimalType type = BIG_DECIMAL.notNull().withPrecisionScale(10, 2);
        assertEquals(10, type.precision());
        assertEquals(2, type.scale());
    }

    @Test
    void precisionMustBeGreaterThanZero() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new BigDecimalType(true, null, 0, 2, null));
        assertEquals("precision must be greater than 0", exception.getMessage());
    }

    @Test
    void scaleMustBeZeroOrPositive() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new BigDecimalType(true, null, 10, -1, null));
        assertEquals("scale must be zero or a positive value", exception.getMessage());
    }

    @Test
    void scaleMustBeLessThanOrEqualToPrecision() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new BigDecimalType(true, null, 5, 6, null));
        assertEquals("scale must be less than or equal to the precision", exception.getMessage());
    }

    @Test
    void scaleMustBeSpecifiedIfPrecisionIsSpecified() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new BigDecimalType(true, null, 10, null, null));
        assertEquals("scale must be specified if precision is specified", exception.getMessage());
    }

    @Test
    void precisionMustBeSpecifiedIfScaleIsSpecified() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new BigDecimalType(true, null, null, 2, null));
        assertEquals("precision must be specified if scale is specified", exception.getMessage());
    }

    @Test
    void nullPrecisionAndScaleAreValid() {
        BigDecimalType type = new BigDecimalType(true, null, null, null, null);
        assertNull(type.precision());
        assertNull(type.scale());
    }
}
