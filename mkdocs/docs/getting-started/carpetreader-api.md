# CarpetReader API

`CarpetReader` provides multiple ways to read data from Parquet files. When you instantiate a `CarpetReader` the file is not opened or read. It's processed when you execute one of its read methods.

To instantiate it you need to provide a Java `File` or a Parquet `InputFile` and the class of the record you want to read. The record class must be a Java record that match the field names in the Parquet schema.


```java
CarpetReader<MyRecord> reader = new CarpetReader<>(inputFile, MyRecord.class);
```

Parquet doesn't support `InputStream` because Parquet's file format requires random access to read metadata from the footer and data pages throughout the file. Since `InputStream` only provides sequential forward-only access, it's not suitable for reading Parquet files.

## Reading Methods

### Stream Processing

```java
Stream<T> stream()
```

`CarpetReader<T>` can return a Java stream to iterate it applying functional logic to filter and transform its content.

```java
var reader = new CarpetReader<>(file, MyRecord.class);
List<OtherType> list = reader.stream()
    .filter(r -> r.value() > 100.0)
    .map(this::mapToOtherType)
    .toList();
```

File content is read while streaming, not loaded entirely into memory. This is useful for large files. The stream will be closed automatically when the processing is done.

### Collecting `toList`

If you don't need to filter or convert the content, you can directly collect the whole content as a `List<T>`:

```java
List<MyRecord> list = new CarpetReader<>(file, MyRecord.class).toList();
```

### For-Each Loop

`CarpetReader<T>` implements `Iterable<T>` and thanks to [For-Each Loop](https://docs.oracle.com/javase/8/docs/technotes/guides/language/foreach.html) feature from Java sintax you can iterate it with a simple for:

```java
var reader = new CarpetReader<>(file, MyRecord.class);
for (MyRecord r: reader) {
    doSomething(r);
}
```

### Iterator

Implementing `Iterable<T>`, there is also available a method `iterator()`:

```java
var reader = new CarpetReader<>(file, MyRecord.class);
Iterator<MyRecord> iterator = reader.iterator();
while (iterator.hasNext()) {
    MyRecord r = iterator.next();
    doSomething(r);
}
```
