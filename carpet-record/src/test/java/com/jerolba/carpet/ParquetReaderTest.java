package com.jerolba.carpet;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;

import com.jerolba.carpet.filestream.FileSystemInputFile;
import com.jerolba.carpet.filestream.FileSystemOutputFile;

public class ParquetReaderTest {

    public enum Flag {
        STRICT_NUMERIC_TYPE, IGNORE_UNKNOWN;
    }

    private final Schema schema;
    private String path;

    public ParquetReaderTest(Schema schema) {
        String fileName = schema.getName() + ".parquet";
        this.path = "/tmp/" + fileName;
        try {
            java.nio.file.Path targetPath = Files.createTempFile("parquet", fileName);
            this.path = targetPath.toFile().getAbsolutePath();
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.schema = schema;
        new File(path).delete();
    }

    public void writer(WriterConsumer writerConsumer) throws IOException {
        OutputFile output = new FileSystemOutputFile(new File(path));
        try (ParquetWriter<Record> writer = AvroParquetWriter.<Record>builder(output)
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withValidation(true)
                .withWriteMode(Mode.OVERWRITE)
                .build()) {
            writerConsumer.accept(writer);
        }
    }

    public <T> ParquetReader<T> carpetReader(Class<T> type) throws IOException {
        InputFile inputFile = new FileSystemInputFile(new File(path));
        ParquetReader<T> carpetReader = CarpetReader.builder(inputFile, type).build();
        return carpetReader;
    }

    @FunctionalInterface
    public interface WriterConsumer {
        void accept(ParquetWriter<Record> writer) throws IOException;
    }
}