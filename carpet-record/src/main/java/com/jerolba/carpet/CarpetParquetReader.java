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
package com.jerolba.carpet;

import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.InputFile;

import com.jerolba.carpet.impl.read.CarpetReadConfiguration;
import com.jerolba.carpet.impl.read.CarpetReadSupport;

public class CarpetParquetReader {

    public static final boolean DEFAULT_FAIL_ON_MISSING_COLUMN = true;
    public static final boolean DEFAULT_FAIL_ON_NULL_FOR_PRIMITIVES = false;
    public static final boolean DEFAULT_FAIL_NARROWING_PRIMITIVE_CONVERSION = false;
    public static final FieldMatchingStrategy DEFAULT_FIELD_MATCHING_STRATEGY = FieldMatchingStrategy.FIELD_NAME;

    private CarpetParquetReader() {
    }

    public static <T> Builder<T> builder(InputFile file, Class<T> readClass) {
        return new Builder<>(file, readClass);
    }

    public static class Builder<T> extends ParquetReader.Builder<T> {

        private final Class<T> readClass;
        private boolean failOnMissingColumn = DEFAULT_FAIL_ON_MISSING_COLUMN;
        private boolean failOnNullForPrimitives = DEFAULT_FAIL_ON_NULL_FOR_PRIMITIVES;
        private boolean failNarrowingPrimitiveConversion = DEFAULT_FAIL_NARROWING_PRIMITIVE_CONVERSION;
        private FieldMatchingStrategy fieldMatchingStrategy = DEFAULT_FIELD_MATCHING_STRATEGY;

        private Builder(InputFile file, Class<T> readClass) {
            super(file, new PlainParquetConfiguration());
            this.readClass = readClass;
        }

        /**
         * Feature that determines whether encountering of missed parquet column should
         * result in a failure (by throwing a RecordTypeConversionException) or not.
         *
         * Feature is enabled by default.
         *
         * @param failOnMissingColumn
         * @return Carpet Reader Builder
         */
        public Builder<T> failOnMissingColumn(boolean failOnMissingColumn) {
            this.failOnMissingColumn = failOnMissingColumn;
            return this;
        }

        /**
         * Feature that determines whether encountering null is an error when
         * deserializing into Java primitive types (like 'int' or 'double'). If it is, a
         * RecordTypeConversionException is thrown to indicate this; if not, default
         * value is used (0 for 'int', 0.0 for double, same defaulting as what JVM
         * uses).
         *
         * Feature is disabled by default.
         *
         * @param failOnNullForPrimitives
         * @return Carpet Reader Builder
         */
        public Builder<T> failOnNullForPrimitives(boolean failOnNullForPrimitives) {
            this.failOnNullForPrimitives = failOnNullForPrimitives;
            return this;
        }

        /**
         * Feature that determines whether coercion from one number type to other number
         * type with less resolutions is allowed or not. If disabled, coercion truncates
         * value.
         *
         * A narrowing primitive conversion may lose information about the overall
         * magnitude of a numeric value and may also lose precision and range. Narrowing
         * follows
         * <a href="https://docs.oracle.com/javase/specs/jls/se10/html/jls-5.html">Java
         * Language Specification</a>
         *
         * Feature is disabled by default.
         *
         * @param failNarrowingPrimitiveConversion
         * @return Carpet Reader Builder
         */
        public Builder<T> failNarrowingPrimitiveConversion(boolean failNarrowingPrimitiveConversion) {
            this.failNarrowingPrimitiveConversion = failNarrowingPrimitiveConversion;
            return this;
        }

        public Builder<T> fieldMatchingStrategy(FieldMatchingStrategy fieldMatchingStrategy) {
            this.fieldMatchingStrategy = fieldMatchingStrategy;
            return this;
        }

        @Override
        protected ReadSupport<T> getReadSupport() {
            CarpetReadConfiguration configuration = new CarpetReadConfiguration(
                    failOnMissingColumn,
                    failNarrowingPrimitiveConversion,
                    failOnNullForPrimitives,
                    fieldMatchingStrategy);
            return new CarpetReadSupport<>(readClass, configuration);
        }

    }

}
