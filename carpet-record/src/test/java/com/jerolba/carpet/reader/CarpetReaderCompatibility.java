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
package com.jerolba.carpet.reader;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData.Record;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.jerolba.carpet.CarpetParquetReader;
import com.jerolba.carpet.ParquetReaderTest;
import com.jerolba.carpet.filestream.FileSystemInputFile;
import com.jerolba.carpet.filestream.FileSystemOutputFile;

class CarpetReaderCompatibility {

    @Nested
    class AvroCollections {

        record Child(String id, int quantity) {
        }

        record AvroCollection(String name, List<Child> children) {
        }

        Schema childSchema = SchemaBuilder.record("Child")
                .fields()
                .requiredString("id")
                .requiredInt("quantity")
                .endRecord();
        Schema schema = SchemaBuilder.record("AvroCollection")
                .fields()
                .requiredString("name")
                .name("children").type().array().items(childSchema).noDefault()
                .endRecord();

        private Record createRecord() {
            Record child1 = new Record(childSchema);
            child1.put("id", "iPad");
            child1.put("quantity", 10);
            Record child2 = new Record(childSchema);
            child2.put("id", "iPhone");
            child2.put("quantity", 20);
            Record record = new Record(schema);
            record.put("name", "Apple");
            record.put("children", List.of(child1, child2));
            return record;
        }

        @Test
        public void defaultTwoLevelAvroCollection() throws IOException {

            File file = new File(ParquetReaderTest.getTestFilePath("defaultAvroCollection"));
            OutputFile output = new FileSystemOutputFile(file);
            try (ParquetWriter<Record> writer = AvroParquetWriter.<Record>builder(output)
                    .withSchema(schema)
                    .build()) {
                writer.write(createRecord());
            }

            InputFile inputFile = new FileSystemInputFile(file);
            try (var carpetReader = CarpetParquetReader.builder(inputFile, AvroCollection.class).build()) {
                var expected = new AvroCollection("Apple", List.of(new Child("iPad", 10), new Child("iPhone", 20)));
                assertEquals(expected, carpetReader.read());
            }
        }

        @Test
        public void threeLevelAvroCollection() throws IOException {
            File file = new File(ParquetReaderTest.getTestFilePath("threeLevelAvroCollection"));
            OutputFile output = new FileSystemOutputFile(file);
            try (ParquetWriter<Record> writer = AvroParquetWriter.<Record>builder(output)
                    .withSchema(schema)
                    .config(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "false")
                    .build()) {
                writer.write(createRecord());
            }

            InputFile inputFile = new FileSystemInputFile(file);
            try (var carpetReader = CarpetParquetReader.builder(inputFile, AvroCollection.class).build()) {
                var expected = new AvroCollection("Apple", List.of(new Child("iPad", 10), new Child("iPhone", 20)));
                assertEquals(expected, carpetReader.read());
            }
        }
    }

}
