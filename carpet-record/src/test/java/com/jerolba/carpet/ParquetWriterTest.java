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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.util.Collection;
import java.util.List;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.schema.MessageType;

import com.jerolba.carpet.CarpetParquetReader.Builder;
import com.jerolba.carpet.io.FileSystemInputFile;
import com.jerolba.carpet.io.OutputStreamOutputFile;
import com.jerolba.carpet.model.WriteRecordModelType;

public class ParquetWriterTest<T> {

    private final Class<T> type;
    private String path;
    private AnnotatedLevels level = AnnotatedLevels.THREE;
    private ColumnNamingStrategy nameStrategy = ColumnNamingStrategy.FIELD_NAME;
    private TimeUnit timeUnit = TimeUnit.MILLIS;
    private Integer precision;
    private Integer scale;
    private RoundingMode roundingMode;

    public ParquetWriterTest(Class<T> type) {
        String fileName = type.getName() + ".parquet";
        this.path = "/tmp/" + fileName;
        try {
            java.nio.file.Path targetPath = Files.createTempFile("parquet", fileName);
            this.path = targetPath.toFile().getAbsolutePath();
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.type = type;
        getTestFile().delete();
    }

    public ParquetWriterTest<T> withLevel(AnnotatedLevels level) {
        this.level = level;
        return this;
    }

    public ParquetWriterTest<T> withNameStrategy(ColumnNamingStrategy nameStrategy) {
        this.nameStrategy = nameStrategy;
        return this;
    }

    public ParquetWriterTest<T> withTimeUnit(TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
        return this;
    }

    public ParquetWriterTest<T> withDecimalConfig(int precision, int scale) {
        this.precision = precision;
        this.scale = scale;
        return this;
    }

    public ParquetWriterTest<T> withBigDecimalScaleAdjustment(RoundingMode roundingMode) {
        this.roundingMode = roundingMode;
        return this;
    }

    public void write(T... values) throws IOException {
        write(List.of(values));
    }

    public void write(WriteRecordModelType<T> mapper, T... values) throws IOException {
        write(mapper, List.of(values));
    }

    public void write(Collection<T> values) throws IOException {
        this.write(null, values);
    }

    public void write(WriteRecordModelType<T> mapper, Collection<T> values) throws IOException {
        OutputStreamOutputFile output = new OutputStreamOutputFile(new FileOutputStream(path));
        var builder = CarpetParquetWriter.builder(output, type)
                .withLevelStructure(level)
                .enableValidation()
                .withColumnNamingStrategy(nameStrategy)
                .withDefaultTimeUnit(timeUnit);
        if (mapper != null) {
            builder = builder.withWriteRecordModel(mapper);
        }
        if (precision != null) {
            builder = builder.withDefaultDecimal(precision, scale)
                    .withBigDecimalScaleAdjustment(roundingMode);
        }
        try (ParquetWriter<T> writer = builder.build()) {
            for (var v : values) {
                writer.write(v);
            }
        }
    }

    public ParquetReader<GenericRecord> getAvroGenericRecordReader() throws IOException {
        return getAvroGenericRecordReaderWithModel(GenericData.get());
    }

    public ParquetReader<GenericRecord> getAvroGenericRecordReaderWithModel(GenericData model) throws IOException {
        return AvroParquetReader
                .<GenericRecord>builder(new FileSystemInputFile(getTestFile()), new PlainParquetConfiguration())
                .withDataModel(model)
                .build();
    }

    public ParquetReader<T> getCarpetReader() throws IOException {
        return getCarpetReader(type);
    }

    public <R> ParquetReader<R> getCarpetReader(Class<R> readType, ReadFlag... flags) throws IOException {
        Builder<R> builder = CarpetParquetReader.builder(new FileSystemInputFile(getTestFile()), readType);
        for (ReadFlag f : flags) {
            if (f.equals(ReadFlag.DONT_FAIL_ON_MISSING_COLUMN)) {
                builder = builder.failOnMissingColumn(false);
            }
            if (f.equals(ReadFlag.FAIL_ON_NULL_FOR_PRIMITIVES)) {
                builder = builder.failOnNullForPrimitives(true);
            }
            if (f.equals(ReadFlag.FAIL_NARROWING_PRIMITIVE_CONVERSION)) {
                builder = builder.failNarrowingPrimitiveConversion(true);
            }
        }
        return builder.build();
    }

    public MessageType getSchema() throws IOException {
        var options = ParquetReadOptions.builder().build();
        try (ParquetFileReader reader = new ParquetFileReader(new FileSystemInputFile(getTestFile()), options)) {
            FileMetaData metaData = reader.getFileMetaData();
            return metaData.getSchema();
        }
    }

    public File getTestFile() {
        return new File(path);
    }
}