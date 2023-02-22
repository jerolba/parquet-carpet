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
import java.nio.file.Files;
import java.util.Collection;
import java.util.List;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.schema.MessageType;

import com.jerolba.carpet.CarpetReader.Builder;
import com.jerolba.carpet.filestream.FileSystemInputFile;
import com.jerolba.carpet.filestream.OutputStreamOutputFile;

public class ParquetWriterTest<T> {

    public enum Flag {
        STRICT_NUMERIC_TYPE, IGNORE_UNKNOWN;
    }

    private final Class<T> type;
    private String path;
    private AnnotatedLevels level = AnnotatedLevels.THREE;

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
        new File(path).delete();
    }

    public ParquetWriterTest(String path, Class<T> type) {
        this.path = path;
        this.type = type;
        new File(path).delete();
    }

    public ParquetWriterTest<T> withLevel(AnnotatedLevels level) {
        this.level = level;
        return this;
    }

    public void write(T... values) throws IOException {
        write(List.of(values));
    }

    public void write(Collection<T> values) throws IOException {
        OutputStreamOutputFile output = new OutputStreamOutputFile(new FileOutputStream(path));
        try (ParquetWriter<T> writer = CarpetWriter.builder(output, type)
                .levelStructure(level)
                .enableValidation()
                .build()) {
            for (var v : values) {
                writer.write(v);
            }
        }
    }

    public ParquetReader<GenericRecord> getAvroGenericRecordReader() throws IOException {
        return AvroParquetReader.<GenericRecord>builder(new FileSystemInputFile(new File(path)))
                .withDataModel(GenericData.get())
                .build();
    }

    public ParquetReader<T> getCarpetReader() throws IOException {
        return getCarpetReader(type);
    }

    public <T> ParquetReader<T> getCarpetReader(Class<T> readType, Flag... flags) throws IOException {
        Builder<T> builder = CarpetReader.builder(new FileSystemInputFile(new File(path)), readType);
        for (Flag f : flags) {
            if (f.equals(Flag.IGNORE_UNKNOWN)) {
                builder = builder.ignoreUnknown(true);
            }
            if (f.equals(Flag.STRICT_NUMERIC_TYPE)) {
                builder = builder.strictNumericType(true);
            }
        }
        return builder.build();
    }

    public MessageType getSchema() throws IOException {
        var options = ParquetReadOptions.builder().build();
        ParquetFileReader reader = new ParquetFileReader(new FileSystemInputFile(new File(path)), options);
        FileMetaData metaData = reader.getFileMetaData();
        return metaData.getSchema();
    }
}