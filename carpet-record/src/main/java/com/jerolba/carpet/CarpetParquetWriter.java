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

import static com.jerolba.carpet.impl.write.DecimalConfig.decimalConfig;
import static java.util.Objects.requireNonNull;

import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.thirdparty.com.google.common.annotations.Beta;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;

import com.jerolba.carpet.impl.write.CarpetWriteConfiguration;
import com.jerolba.carpet.impl.write.DecimalConfig;
import com.jerolba.carpet.impl.write.WriteSupportFactory;
import com.jerolba.carpet.model.WriteRecordModelType;

public class CarpetParquetWriter {

    private CarpetParquetWriter() {
    }

    public static <T> Builder<T> builder(OutputFile file, Class<T> recordClass) {
        return new Builder<>(file, recordClass);
    }

    public static class Builder<T> extends ParquetWriter.Builder<T, Builder<T>> {

        private final Class<T> recordClass;
        private final Map<String, String> extraMetaData = new HashMap<>();
        private WriteModelFactory<T> writeModelFactory;
        private AnnotatedLevels annotatedLevels = AnnotatedLevels.THREE;
        private ColumnNamingStrategy columnNamingStrategy = ColumnNamingStrategy.FIELD_NAME;
        private TimeUnit defaultTimeUnit = TimeUnit.MILLIS;
        private DecimalConfig decimalConfig = decimalConfig();

        private Builder(OutputFile file, Class<T> recordClass) {
            super(file);
            this.recordClass = recordClass;
        }

        /**
         * Adds to writer metadata to include in the generated parquet file.
         *
         * @param extraMetaData to add
         * @return this builder for method chaining.
         */
        @Override
        public Builder<T> withExtraMetaData(Map<String, String> extraMetaData) {
            this.extraMetaData.putAll(extraMetaData);
            return this;
        }

        /**
         * Adds to writer metadata to include in the generated parquet file.
         *
         * @param key   of the metadata to add
         * @param value of the metadata to add
         * @return this builder for method chaining.
         */
        public Builder<T> withExtraMetaData(String key, String value) {
            this.extraMetaData.put(key, value);
            return this;
        }

        @Override
        protected Builder<T> self() {
            return this;
        }

        /**
         * Sets the type of collections type that will be generated following the
         * <a href=
         * "https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists">LogicalTypes
         * definition</a>
         *
         * If not configured, 3-level structure is used
         *
         * @param annotatedLevels an Enum configuring the number of levels
         * @return this builder for method chaining.
         */
        public Builder<T> withLevelStructure(AnnotatedLevels annotatedLevels) {
            requireNonNull(annotatedLevels, "Annotated levels can not be null");
            this.annotatedLevels = annotatedLevels;
            return self();
        }

        /**
         * Sets the strategy to use generating parquet message and record field names in
         * the schema
         *
         * @param columnNamingStrategy an Enum configuring the strategy to use
         * @return this builder for method chaining.
         */
        public Builder<T> withColumnNamingStrategy(ColumnNamingStrategy columnNamingStrategy) {
            requireNonNull(columnNamingStrategy, "Column naming strategy can not be null");
            this.columnNamingStrategy = columnNamingStrategy;
            return self();
        }

        /**
         * Sets the time unit resolution writing TIME or TIMESTAMP fields:
         * <ul>
         * <li>milliseconds</li>
         * <li>microseconds</li>
         * <li>nanoseconds</li>
         *
         * @param defaultTimeUnit an Enum configuring the resolution to use
         * @return this builder for method chaining.
         */
        public Builder<T> withDefaultTimeUnit(TimeUnit defaultTimeUnit) {
            requireNonNull(defaultTimeUnit, "Default time unit can not be null");
            this.defaultTimeUnit = defaultTimeUnit;
            return self();
        }

        /**
         * Sets Decimal precision and scale
         *
         * @param precision of the decimal number
         * @param scale     of the decimal number
         * @return this builder for method chaining.
         */
        public Builder<T> withDefaultDecimal(int precision, int scale) {
            this.decimalConfig = decimalConfig.withPrecisionAndScale(precision, scale);
            return self();
        }

        /**
         * Sets the rounding mode to use when adjusting the scale of a BigDecimal
         *
         * @param roundingMode to use
         * @return this builder for method chaining.
         */
        public Builder<T> withBigDecimalScaleAdjustment(RoundingMode roundingMode) {
            this.decimalConfig = decimalConfig.withRoundingMode(roundingMode);
            return this;
        }

        /**
         * Configures the factory of the write data model to use, instead of default
         * record convention. The factory receives all configuration to decide how to
         * build the WriteRecordModelType.
         *
         * Experimental feature to support custom data models different from record,
         * like classes or DataFrames
         *
         * @param writeModelFactory creates WriteRecordModelType given configuration
         *                          specific to Carpet and Parquet
         * @return this builder for method chaining.
         */
        @Beta
        public Builder<T> withWriteRecordModel(WriteModelFactory<T> writeModelFactory) {
            this.writeModelFactory = writeModelFactory;
            return self();
        }

        /**
         * Configures write data model to use, instead of default record convention.
         *
         * Experimental feature to support custom data models different from record,
         * like classes or DataFrames
         *
         * @param rootWriteRecordModel write record model to use
         * @return this builder for method chaining.
         */
        @Beta
        public Builder<T> withWriteRecordModel(WriteRecordModelType<T> rootWriteRecordModel) {
            if (!rootWriteRecordModel.getClassType().equals(recordClass)) {
                throw new IllegalArgumentException("Root Write record Model class ("
                        + rootWriteRecordModel.getClassType() + ") is not equals to configured Carpet Writer class ("
                        + recordClass + ")");
            }
            return withWriteRecordModel((writeClass, writeConfigurationContext) -> rootWriteRecordModel);
        }

        @Override
        protected WriteSupport<T> getWriteSupport(ParquetConfiguration parquetConfig) {
            CarpetWriteConfiguration carpetCfg = new CarpetWriteConfiguration(
                    annotatedLevels,
                    columnNamingStrategy,
                    defaultTimeUnit,
                    decimalConfig);
            return WriteSupportFactory.createWriteSupport(recordClass, extraMetaData,
                    parquetConfig, carpetCfg, writeModelFactory);
        }

        @Override
        protected WriteSupport<T> getWriteSupport(Configuration conf) {
            return getWriteSupport(new PlainParquetConfiguration(conf.getPropsWithPrefix("")));
        }

    }

}
