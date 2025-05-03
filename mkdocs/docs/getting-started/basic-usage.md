# Basic Usage

## Creating Records

To use Carpet, start by defining your data structure using Java records. You don't need to generate classes or inherit from Carpet classes:

```java
record MyRecord(long id, String name, int size, double value, double percentile) { }
```

Carpet provides a writer and a reader with a default configuration and convenience methods.

## Writing to Parquet

Carpet can use reflection to define the Parquet file schema and writes all the content of your objects into the file:

```java
List<MyRecord> data = calculateDataToPersist();

try (OutputStream outputStream = new FileOutputStream("my_file.parquet")) {
    try (CarpetWriter<MyRecord> writer = new CarpetWriter<>(outputStream, MyRecord.class)) {
        writer.write(data);
    }
}
```

## Reading from Parquet

To read a Parquet file, you just need to provide a File and Record class that matches the Parquet schema:

```java
List<MyRecord> data = new CarpetReader<>(new File("my_file.parquet"), MyRecord.class).toList();
```

### Reading as Map

If you don't know the schema of the file, or a Map is valid for your use case, you can deserialize to `Map<String, Object>`:

```java
List<Map> data = new CarpetReader<>(new File("my_file.parquet"), Map.class).toList();
```

## Next Steps

Once you're familiar with the basics, you can explore more advanced features:

- [Writer API](../getting-started/carpetwriter-api.md) for detailed write operations
- [Reader API](../getting-started/carpetreader-api.md) for reading capabilities
- [Data Types](../advanced/data-types.md) for supported data types and nested structures
- [Configuration](../advanced/configuration.md) for customizing Parquet settings