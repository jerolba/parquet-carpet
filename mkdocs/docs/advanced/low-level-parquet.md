# Low level Parquet classes

Carpet is built on top of [parquet-java](https://github.com/apache/parquet-java/) library and supports creating native library `ParquetWriter` and `ParquetReader` classes, and use it with third party libraries that work with Parquet classes.

#### ParquetWriter

```java
List<MyRecord> data = calculateDataToPersist();

Path path = new org.apache.hadoop.fs.Path("my_file.parquet");
OutputFile outputFile = HadoopOutputFile.fromPath(path, new Configuration());
try (ParquetWriter<MyRecord> writer = CarpetParquetWriter.builder(outputFile, MyRecord.class)
        .withWriteMode(Mode.OVERWRITE)
        .withCompressionCodec(CompressionCodecName.GZIP)
        .withPageRowCountLimit(100_000)
        .withBloomFilterEnabled("name", true)
        .build()) {

    otherLibraryIntegrationWrite(writer, data);
}
```

#### ParquetReader

```java
Path path = new org.apache.hadoop.fs.Path("my_file.parquet");
InputFile inputFile = new HadoopInputFile(path, new Configuration());
try (ParquetReader<MyRecord> reader = CarpetParquetReader.builder(inputFile, MyRecord.class).build()) {
    var data = otherLibraryIntegrationRead(reader);
    // process data
}
```