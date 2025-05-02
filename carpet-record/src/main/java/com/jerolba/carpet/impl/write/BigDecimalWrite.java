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

import static com.jerolba.carpet.impl.write.DecimalConfig.decimalConfig;
import static java.math.RoundingMode.UNNECESSARY;

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;

import com.jerolba.carpet.RecordTypeConversionException;
import com.jerolba.carpet.annotation.PrecisionScale;
import com.jerolba.carpet.annotation.Rounding;

class BigDecimalWrite {

    enum DecimalMapper {
        INT, LONG, BINARY
    }

    private final Integer precision;
    private final Integer scale;
    private final RoundingMode roundingMode;
    private final DecimalMapper mapper;

    public BigDecimalWrite(DecimalConfig decimalConfig) {
        this.precision = decimalConfig.precision();
        this.scale = decimalConfig.scale();
        this.roundingMode = decimalConfig.roundingMode();
        this.mapper = calcMapper(decimalConfig.precision());
    }

    public void write(RecordConsumer recordConsumer, Object value) {
        BigDecimal dec = rescaleIfNeeded((BigDecimal) value);
        switch (mapper) {
        case INT:
            recordConsumer.addInteger(dec.unscaledValue().intValue());
            break;
        case LONG:
            recordConsumer.addLong(dec.unscaledValue().longValue());
            break;
        case BINARY:
            byte[] a = dec.unscaledValue().toByteArray();
            recordConsumer.addBinary(Binary.fromConstantByteArray(a));
            break;
        }
    }

    private static DecimalMapper calcMapper(int precision) {
        if (precision <= 9) {
            return DecimalMapper.INT;
        }
        if (precision <= 18) {
            return DecimalMapper.LONG;
        }
        return DecimalMapper.BINARY;
    }

    // From org.apache.avro.Conversions$DecimalConversion::validate
    private BigDecimal rescaleIfNeeded(BigDecimal value) {
        int valueScale = value.scale();
        boolean scaleAdjusted = false;
        if (valueScale != scale) {
            try {
                if (roundingMode != null) {
                    value = value.setScale(scale, roundingMode);
                    scaleAdjusted = true;
                } else {
                    value = value.setScale(scale, UNNECESSARY);
                    scaleAdjusted = true;
                }
            } catch (ArithmeticException aex) {
                throw new RecordTypeConversionException(
                        "Cannot encode BigDecimal with scale " + valueScale + " as scale " + scale
                                + " without rounding");
            }
        }
        int valuePrecision = value.precision();
        if (valuePrecision > precision) {
            if (scaleAdjusted) {
                throw new RecordTypeConversionException(
                        "Cannot encode BigDecimal with precision " + valuePrecision + " as max precision "
                                + precision + ". This is after safely adjusting scale from " + valueScale
                                + " to required " + scale);
            } else {
                throw new RecordTypeConversionException(
                        "Cannot encode BigDecimal with precision " + valuePrecision + " as max precision "
                                + precision);
            }
        }
        return value;
    }

    static DecimalConfig buildDecimalConfig(PrecisionScale precisionAndScale, Rounding rounding,
            DecimalConfig decimalConfig) {
        Integer precision = precisionAndScale != null ? precisionAndScale.precision() : null;
        Integer scale = precisionAndScale != null ? precisionAndScale.scale() : null;
        RoundingMode roundingMode = rounding != null ? rounding.value() : null;
        return buildDecimalConfig(precision, scale, roundingMode, decimalConfig);
    }

    static DecimalConfig buildDecimalConfig(Integer precision, Integer scale, RoundingMode roundingMode,
            DecimalConfig decimalConfig) {
        if (decimalConfig == null) {
            decimalConfig = decimalConfig();
        }
        if (precision != null && scale != null) {
            decimalConfig = decimalConfig.withPrecisionAndScale(precision, scale);
        }
        if (roundingMode != null) {
            decimalConfig = decimalConfig.withRoundingMode(roundingMode);
        }
        return decimalConfig;
    }

}
