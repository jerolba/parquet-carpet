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
import java.io.IOException;
import java.nio.file.Files;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;

import com.jerolba.carpet.CarpetParquetReader.Builder;
import com.jerolba.carpet.io.FileSystemInputFile;
import com.jerolba.carpet.io.FileSystemOutputFile;

public class ParquetReaderTest {

    private final Schema schema;
    private final String path;

    public ParquetReaderTest(Schema schema) {
        this.path = getTestFilePath(schema.getName());
        this.schema = schema;
    }

    public static String getTestFilePath(String name) {
        String fileName = name + ".parquet";
        var path = "/tmp/" + fileName;
        try {
            java.nio.file.Path targetPath = Files.createTempFile("parquet", fileName);
            path = targetPath.toFile().getAbsolutePath();
        } catch (IOException e) {
            e.printStackTrace();
        }
        new File(path).delete();
        return path;
    }

    public void writer(WriterConsumer writerConsumer) throws IOException {
        writerWithModel(GenericData.get(), writerConsumer);
    }

    public void writerWithModel(GenericData model, WriterConsumer writerConsumer) throws IOException {
        OutputFile output = new FileSystemOutputFile(new File(path));
        try (ParquetWriter<Record> writer = AvroParquetWriter.<Record>builder(output)
                .withSchema(schema)
                .withDataModel(model)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withValidation(true)
                .withWriteMode(Mode.OVERWRITE)
                .build()) {
            writerConsumer.accept(writer);
        }
    }

    public <T> ParquetReader<T> getCarpetReader(Class<T> readType, ReadFlag... flags) throws IOException {
        InputFile inputFile = new FileSystemInputFile(new File(path));
        Builder<T> builder = CarpetParquetReader.builder(inputFile, readType);
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

    @FunctionalInterface
    public interface WriterConsumer {
        void accept(ParquetWriter<Record> writer) throws IOException;
    }
}