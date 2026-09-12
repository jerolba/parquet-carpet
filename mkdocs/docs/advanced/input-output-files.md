# Input and Output Files

`parquet-java` defines `OutputFile` and `InputFile` interfaces to interact with files. Originally, it only provided `HadoopOutputFile` and `HadoopInputFile` implementations that were capable of working with Hadoop and local files.

This required a Hadoop dependency to be included in the project. This is not ideal for projects that only need to work with local files, as it adds unnecessary complexity and size to the project. To address this, Parquet Java recently added `LocalOutputFile` and `LocalInputFile` implementations.

Before these classes were created, Carpet provided a local file implementation with `FileSystemOutputFile` and `FileSystemInputFile`. You can use either implementation.

As a different dependency, Carpet provides implementations for S3 files with `S3OutputFile` and `S3InputFile`, usable with Carpet or any other Parquet Java library.

## Local Files

### Writing local files

```java

// Carpet provides a local file implementation with FileSystemOutputFile
OutputFile outputFile = new FileSystemOutputFile(Path.of("/tmp/data.parquet"));
// Parquet Java provides a local file implementation with LocalOutputFile
OutputFile outputFile = new LocalOutputFile(Path.of("/tmp/data.parquet"));

try (CarpetWriter<Data> writer = new CarpetWriter<>(outputFile, Data.class)) {
    writer.write(new Data("123"));
}

```

### Writing to an OutputStream

Carpet also provides an `OutputStreamOutputFile` implementation that allows you to write Parquet files to any `OutputStream`. This is useful for writing to network streams, in-memory buffers, or other custom destinations.

```java

ByteArrayOutputStream baos = new ByteArrayOutputStream();
OutputStreamOutputFile outputStreamOutputFile = new OutputStreamOutputFile(baos);
try (CarpetWriter<Data> writer = new CarpetWriter<>(outputStreamOutputFile, Data.class)) {
    writer.write(new Data("456"));
}

```

### Reading local files

```java
// Carpet provides a local file implementation with FileSystemInputFile
InputFile inputFile = new FileSystemInputFile(Path.of("/tmp/data.parquet"));
// Parquet Java provides a local file implementation with LocalInputFile
InputFile inputFile = new LocalInputFile(Path.of("/tmp/data.parquet"));

CarpetReader<Data> reader = new CarpetReader<>(inputFile, Data.class);
List<Data> data = reader.stream().toList();
```

## S3 Files

`carpet-s3` adds the ability to read and write Parquet files directly from Amazon S3 or other S3-compatible services. It provides `S3InputFile` and `S3OutputFile` implementations that handle all the complexity of S3 access.

It's based on the [AWS S3 SDK](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/home.html) and the dependency is added automatically when you include `carpet-s3` in your project.

Add the `carpet-s3` dependency to your project:

--8<-- "carpet-s3-dependency.md"

### Writing to S3

The simplest way to write a Parquet file to S3:

```java
OutputFile outputFile = S3OutputFile.of("s3://my-bucket/output.parquet");
try (CarpetWriter<Data> writer = new CarpetWriter<>(outputFile, Data.class)) {
    writer.write(new Data("123"));
}
```

### Reading from S3

Reading a Parquet file from S3 works the same way:

```java
InputFile inputFile = S3InputFile.of("s3://my-bucket/data.parquet");
CarpetReader<Data> reader = new CarpetReader<>(inputFile, Data.class);
List<Data> data = reader.stream().toList();
```

For more details on S3 files and configuration, check out the [S3 Files](advanced/s3-files.md) guide.