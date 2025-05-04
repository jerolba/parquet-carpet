# CarpetWriter API

`CarpetWriter` provides multiple methods for writing data to Parquet files.

To instantiate it you need to provide an `OutputStream` or a Parquet `OutputFile` and the class of the record you want to write. The record class must be a Java record that match the field names in the Parquet schema.

```java
CarpetWriter<MyRecord> writer = new CarpetWriter<>(outputStream, MyRecord.class);
```

## Writing Methods

* `void write(T value)`: Write a single element. Can be called repeatedly.
* `void accept(T value)`: Implementing `Consumer<T>` interface, write a single element. Created to be used in functional processes. If there is an `IOException`, it is wrapped with a `UncheckedIOException`
* `void write(Collection<T> collection)`: iterates and serializes a whole collection. Can be any type of `Collection` implementation.
* `void write(Stream<T> stream)`: consumes a stream and serializes its values.

You can call repeatedly to all methods in any combination if needed.

## Usage Example

```java
var outputFile = new FileSystemOutputFile(new File("my_file.parquet"));
try (var writer = new CarpetWriter<MyRecord>(outputStream, MyRecord.class)) {
    // Write single element
    writer.write(new MyRecord("foo"));

    // Write collection
    writer.write(List.of(new MyRecord("bar")));

    // Write stream
    writer.write(Stream.of(new MyRecord("foobar")));
}
```

`CarpetWriter` needs to be closed, and implements `Closeable` interface to be used in try-with-resources.
