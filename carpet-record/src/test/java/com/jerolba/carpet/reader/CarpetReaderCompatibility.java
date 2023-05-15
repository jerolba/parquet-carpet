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
import java.net.URL;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.jerolba.carpet.CarpetParquetReader;
import com.jerolba.carpet.CarpetParquetWriter;
import com.jerolba.carpet.ParquetReaderTest;
import com.jerolba.carpet.io.FileSystemInputFile;
import com.jerolba.carpet.io.FileSystemOutputFile;

class CarpetReaderCompatibility {

    @Nested
    class AvroCollections {

        record Child(String id, int quantity) {
        }

        record WithCollection(String name, List<Child> children) {
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

        @Nested
        class TwoLevelAvroCollection {

            private final File file = new File(ParquetReaderTest.getTestFilePath("defaultAvroCollection"));

            @BeforeEach
            void writeContent() throws IOException {
                OutputFile output = new FileSystemOutputFile(file);
                try (ParquetWriter<Record> writer = AvroParquetWriter.<Record>builder(output)
                        .withSchema(schema)
                        .build()) {
                    writer.write(createRecord());
                }
            }

            @Test
            void canBeReadByCarpet() throws IOException {
                InputFile inputFile = new FileSystemInputFile(file);
                try (var carpetReader = CarpetParquetReader.builder(inputFile, WithCollection.class).build()) {
                    var expected = new WithCollection("Apple", List.of(new Child("iPad", 10), new Child("iPhone", 20)));
                    assertEquals(expected, carpetReader.read());
                }
            }

            @Test
            void canBeReadByAvro() throws IOException {
                InputFile inputFile = new FileSystemInputFile(file);
                try (var carpetReader = AvroParquetReader.<GenericRecord>builder(inputFile)
                        .withDataModel(GenericData.get()).build()) {
                    GenericRecord record = carpetReader.read();
                    assertEquals("Apple", record.get("name").toString());
                    var array = (Array<GenericRecord>) record.get("children");
                    assertEquals("iPad", array.get(0).get("id").toString());
                    assertEquals("iPhone", array.get(1).get("id").toString());
                }
            }

        }

        @Nested
        class ThreeLevelAvroCollection {

            private final File file = new File(ParquetReaderTest.getTestFilePath("threeLevelAvroCollection"));

            @BeforeEach
            void writeContent() throws IOException {
                OutputFile output = new FileSystemOutputFile(file);
                try (ParquetWriter<Record> writer = AvroParquetWriter.<Record>builder(output)
                        .withSchema(schema)
                        .config(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "false")
                        .build()) {
                    writer.write(createRecord());
                }
            }

            @Test
            void canBeReadByCarpet() throws IOException {
                InputFile inputFile = new FileSystemInputFile(file);
                try (var carpetReader = CarpetParquetReader.builder(inputFile, WithCollection.class).build()) {
                    var expected = new WithCollection("Apple", List.of(new Child("iPad", 10), new Child("iPhone", 20)));
                    assertEquals(expected, carpetReader.read());
                }
            }

            @Test
            void canBeReadByAvro() throws IOException {
                InputFile inputFile = new FileSystemInputFile(file);
                try (var carpetReader = AvroParquetReader.<GenericRecord>builder(inputFile)
                        .withDataModel(GenericData.get()).build()) {
                    GenericRecord record = carpetReader.read();
                    assertEquals("Apple", record.get("name").toString());
                    var array = (Array<GenericRecord>) record.get("children");
                    assertEquals("iPad", array.get(0).get("id").toString());
                    assertEquals("iPhone", array.get(1).get("id").toString());
                }
            }

        }

        @Nested
        class ThreeLevelCarpetCollection {

            private final File file = new File(ParquetReaderTest.getTestFilePath("threeLevelCarpetCollection"));

            @BeforeEach
            void writeContent() throws IOException {
                OutputFile output = new FileSystemOutputFile(file);
                try (ParquetWriter<WithCollection> writer = CarpetParquetWriter
                        .<WithCollection>builder(output, WithCollection.class)
                        .build()) {
                    var value = new WithCollection("Apple", List.of(new Child("iPad", 10), new Child("iPhone", 20)));
                    writer.write(value);
                }
            }

            @Test
            void canBeReadByCarpet() throws IOException {
                InputFile inputFile = new FileSystemInputFile(file);
                try (var carpetReader = CarpetParquetReader.builder(inputFile, WithCollection.class).build()) {
                    var expected = new WithCollection("Apple", List.of(new Child("iPad", 10), new Child("iPhone", 20)));
                    assertEquals(expected, carpetReader.read());
                }
            }

            @Test
            void avroCollectionHasElementChild() throws IOException {
                // Because schema is not added as metadata can not parse it correctly
                InputFile inputFile = new FileSystemInputFile(file);
                try (var carpetReader = AvroParquetReader.<GenericRecord>builder(inputFile)
                        .withDataModel(GenericData.get()).build()) {
                    GenericRecord record = carpetReader.read();
                    assertEquals("Apple", record.get("name").toString());
                    var array = (Array<GenericRecord>) record.get("children");
                    // NOTE: element item is not expected
                    var element = (GenericRecord) array.get(0).get("element");
                    assertEquals("iPad", element.get("id").toString());
                }
            }

        }

        @Test
        void parquet2069Original() throws IOException {
            URL resource = this.getClass().getClassLoader().getResource("samples/PARQUET-2069.parquet");
            String file = resource.getFile();

            record Distance(String text, Long value) {
            }
            record Duration(String text, Long value) {
            }
            record Row(Distance distance, Duration duration, String status) {
            }
            // Elements is an explicit struct value
            record Elements(List<Row> elements) {
            }
            record Addresses(List<String> destination_addresses, List<String> origin_addresses,
                    List<Elements> rows) {
            }

            InputFile inputFile = new FileSystemInputFile(new File(file));
            try (var carpetReader = CarpetParquetReader.builder(inputFile, Addresses.class).build()) {
                var read = carpetReader.read();

                var destinations = List.of("Washington, DC, USA", "Philadelphia, PA, USA", "Santa Barbara, CA, USA",
                        "Miami, FL, USA", "Austin, TX, USA", "Napa County, CA, USA");
                var origins = List.of("New York, NY, USA");
                var rows = List.of(
                        new Row(new Distance("227 mi", 365468L), new Duration("3 hours 54 mins", 14064L), "OK"),
                        new Row(new Distance("94.6 mi", 152193L), new Duration("1 hour 44 mins", 6227L), "OK"),
                        new Row(new Distance("2,878 mi", 4632197L), new Duration("1 day 18 hours", 151772L), "OK"),
                        new Row(new Distance("1,286 mi", 2069031L), new Duration("18 hours 43 mins", 67405L), "OK"),
                        new Row(new Distance("1,742 mi", 2802972L), new Duration("1 day 2 hours", 93070L), "OK"),
                        new Row(new Distance("2,871 mi", 4620514L), new Duration("1 day 18 hours", 152913L), "OK"));
                var expected = new Addresses(destinations, origins, List.of(new Elements(rows)));
                assertEquals(expected, read);
            }

            // Avro can not parse inner list correctly
            try (var carpetReader = AvroParquetReader.<GenericRecord>builder(inputFile)
                    .withDataModel(GenericData.get()).build()) {
                GenericRecord record = carpetReader.read();
                System.out.println(record);

            }
        }

    }

}
