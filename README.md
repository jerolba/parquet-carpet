[![Build Status](https://github.com/jerolba/parquet-carpet/actions/workflows/build-gradle-project.yml/badge.svg)](https://github.com/jerolba/parquet-carpet/actions)
[![Maven Central](https://img.shields.io/maven-central/v/com.jerolba/carpet-record.svg)](https://maven-badges.herokuapp.com/maven-central/com.jerolba/carpet-record)
[![License](http://img.shields.io/:license-apache-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![javadoc](https://javadoc.io/badge2/com.jerolba/carpet-record/javadoc.svg)](https://javadoc.io/doc/com.jerolba/carpet-record)

# Carpet: Parquet Serialization and Deserialization Library for Java

A Java library for serializing and deserializing Parquet files efficiently using Java records. This library provides a simple and user-friendly API for working with Parquet files, making it easy to read and write data in the Parquet format in your Java applications.

## Features

- Serialize Java records to Parquet files
- Deserialize Parquet files to Java records
- Support nested data structures
- Support nested Collections and Maps
- Very simple API
- Low level configuration of Parquet properties
- Low overhead procesing files
- Minimized `parquet-mr` and hadoop transitive dependencies


## Table of Contents

- [Installation](#installation)
- [Basic Usage](#basic-usage)
- [Advanced Usage](#advanced-usage)
- [Contribute](#contribute)
- [Build](#build)
- [License](#license)

## Installation

You can include this library in your Java project using Maven:

```xml
<dependency>
    <groupId>com.jerolba</groupId>
    <artifactId>carpet-record</artifactId>
    <version>0.0.11</version>
</dependency>
```

or using Gradle:

```gradle
implementation 'com.jerolba:carpet-record:0.0.11'
```

Carpet includes only the essential transitive dependencies required for file read and write operations.

## Basic Usage

To serialize and deserialize Parquet files in your Java application, you just need Java records. You don't need to generate classes or inherit from Carpet classes.

```java
record MyRecord(long id, String name, int size, double value, double percentile)
```

Carpet provides a writer and a reader with a default configuration and convenience methods.

### Serialization

Using reflection, Carpet defines Parquet file schema, and writes all the content of your objects into the file:

```java
List<MyRecord> data = calculateDataToPersist();

try (OutputStream outputStream = new FileOutputStream("my_file.parquet")) {
    try (CarpetWriter<MyRecord> writer = new CarpetWriter<>(outputStream, MyRecord.class)) {
        writer.write(data);
    }
}
```

### Deserialization

You just need to provide a File and Record class that match parquet schema to read:

```java
List<MyRecord> data = new CarpetReader<>(new File("my_file.parquet"), MyRecord.class).toList();
```

If you don't know the schema of the file, or a Map is valid, you can deserialize to `Map<String, Object>`:

```java
List<Map> data = new CarpetReader<>(new File("my_file.parquet"), Map.class).toList();
```

## Advanced Usage

- [CarpetWriter API](#carpetwriter-api)
- [CarpetReader API](#carpetreader-api)
- [Column name mapping](#column-name-mapping)
- [Supported types](#supported-types)
- [Projections](#projections)
- [Nullability](#nullability)
- [Read schema mismatch](#read-schema-mismatch)
- [Parquet configuration tunning](#parquet-configuration-tunning)
- [Column name conversion](#column-name-conversion)
- [Low level Parquet classes](#low-level-parquet-classes)
- [Local file system files](#local-file-system-files)

### CarpetWriter API

`CarpetWriter` provides multiple methods that add information to a file:

* `void write(T value)`: Write a single element, and can be called repeatedly.
* `void accept(T value)`: Implementing Consumer<T> interface, write a single element. Created to be used in functional processes. If there is an `IOException`, it is wrapped with a `UncheckedIOException`
* `void write(Collection<T> collection)`: iterates and serializes the whole collection. Can be any type of Collection implementation.
* `void write(Stream<T> stream)`: consumes the stream and serializes its values.

You can call repeatedly to all methods in any combination if needed.

`CarpetWriter` needs to be closed, and implements `Closeable` interface.

```java
try (OutputStream outputStream = new FileOutputStream("my_file.parquet")) {
    try (CarpetWriter<MyRecord> writer = new CarpetWriter<>(outputStream, MyRecord.class)) {
        writer.write(new MyRecord("foo"));
        writer.write(List.of(new MyRecord("bar")));
        writer.write(Stream.of(new MyRecord("foobar")));
    }
}
```

### CarpetReader API

`CarpetReader` provides multiple ways to read a file. When you instantiate a `CarpetReader` the file is not opened or read. It's processed when you execute one of its read methods.

#### Stream

`CarpetReader<T>` can return a Java stream to iterate it applying functional logic to filter and transform its content.

```java
var reader = new CarpetReader<>(file, MyRecord.class);
List<OtherTpye> list = reader.stream().filter(r -> r.value() > 100.0).map(this::mapToOtherType).toList();
```

File content is not materialized and then streamed. It's read while streamed.

#### toList

If you don't need to filter or convert the content, you can directly get the whole content as a `List<T>`:

```java
List<MyRecord> list = new CarpetReader<>(file, MyRecord.class).toList();
```

#### For-Each Loop

`CarpetReader<T>` implements `Iterable<T>` and thanks to [For-Each Loop](https://docs.oracle.com/javase/8/docs/technotes/guides/language/foreach.html) feature from Java sintax you can iterate it with a simple for:

```java
var reader = new CarpetReader<>(file, MyRecord.class);
for (MyRecord r: reader) {
    doSomething(r);
}
```

#### Iterator

Implementing `Iterable<T>`, there is also available a method `iterator()`:

```java
var reader = new CarpetReader<>(file, MyRecord.class);
Iterator<MyRecord> iterator = reader.iterator();
while (iterator.hasNext()) {
    MyRecord r = iterator.next();
    doSomething(r);
}
```

### Column name mapping

Carpet uses reflection to discover the schema of your files. The name of a Java record attribute is limited by Java syntax, while the name of a Parquet column supports more flexible syntax.

To support non valid names in Java, Carpet defines the annotation `@Alias` on record fields:

```java
record MyRecord(long id, String name, int size, @Alias("$name.id") String nameId){ }
```

### Supported types

Main Java types are mapped to Parquet data types

| Java Type | Parquet Type |
|---|---|
| boolean/Boolean | boolean |
| byte/Byte | int32 |
| short/Short | int32 |
| int/Integer | int32 |
| long/Long | int64 |
| float/Float | float |
| double/Double | double |
| String | binary (STRING) |
| Enum | binary (ENUM) |
| UUID | fixed_len_byte_array(16) (UUID) |

While Parquet supports [Temporal Types](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#temporal-types), they are still not implemented in Carpet. If you need to write a file with date/time, you can use epoch time represented as int64 or formated date as String.

#### Nested records

Carpet supports nested records to create files with structured data. There is one exception: types can not be recursive directly nor indirectly.

```java

record Address(String street, String zip, String city) { }
record Job(String company, String position, int years){ }
record Person(long id, Job job, Address address) { }

try (var outputStream = new FileOutputStream("my_file.parquet")) {
    try (var writer = new CarpetWriter<>(outputStream, Person.class)) {
        var president = new Person(1010101, new Job("USA", POTUS, 3),
            new Address("1600 Pennsylvania Av.", "20500", "Washington"));
        writer.write(president));
    }
}

```
The generated file has this Parquet schema:

```
message Person {
  required int64 id;
  optional group address {
    optional binary street (STRING);
    optional binary zip (STRING);
    optional binary city (STRING);
  }
  optional group job {
    optional binary company (STRING);
    optional binary position (STRING);
    required int32 years;
  }
}
```

Records with recursivity can not be used in Carpet:

```java
record Foo(String id, Foo next) { }

record Child(String id, First recursive) { }
record First(String id, Child child) { }
```


#### Nested collections

Carpet supports nested collections to create files with structured data. Collection elements must be one of the supported types.

```java

record Line(String sku, int quantity, double price){ }
record Invoice(String id, double amount, double taxes, List<Line> lines) { }

try (var outputStream = new FileOutputStream("my_file.parquet")) {
    try (var writer = new CarpetWriter<>(outputStream, Invoice.class)) {
        var invoice = new Invoice("2023/211", 2323.23, 232.32, List.of(
            new Line("AAA", 3, 500.0), new Line("BBB", 1, 823.23)));
        writer.write(invoice);
    }
}

```

The generated file has this Parquet schema:

```
message Invoice {
  optional binary id (STRING);
  required double amount;
  required double taxes;
  optional group lines (LIST) {
    repeated group list {
      optional group element {
        optional binary sku (STRING);
        required int32 quantity;
        required double price;
      }
    }
  }
}
```

You can deserialize an existing file with a collection to any type of Java `Collection` implementation. The only restriction is that the Collection type must have a constructor without parameters.

#### Nested Maps

Carpet supports nested maps to create files with structured data. Map elements must be one of the supported types.

```java
record State(double area, int population){ }
record Country(String name, double area, Map<String, State> states) { }

try (var outputStream = new FileOutputStream("my_file.parquet")) {
    try (var writer = new CarpetWriter<>(outputStream, Country.class)) {
        var country = new Country("USA", 9_833_520.0, Map.of(
            "Idaho", new State(216_444.0, 1_975_000),
            "Texas", new State(695_662.0, 29_145_505)));
        writer.write(country);
    }
}
```

The generated file has this Parquet schema:

```
message Country {
  optional binary name (STRING);
  required double area;
  optional group states (MAP) {
    repeated group key_value {
      required binary key (STRING);
      optional group value {
        required double area;
        required int32 population;
      }
    }
  }
}
```

You can deserialize an existing file with a map to any type of Java `Map` implementation. The only restriction is that the Map type must have a constructor without parameters.

#### Generic Types

Records classes can not have generic elements. Carpet creates the schema from Record information and needs concrete values.

This code throws a `RecordTypeConversionException`:

```java
record WithGeneric<T>(String name, T child) { }

try (var outputStream = new FileOutputStream("my_file.parquet")) {
    try (var writer = new CarpetWriter<>(outputStream, WithGeneric.class)) {
        WithGeneric<String> value = new WithGeneric<>("Foo", "Bar");
        writer.write(country);
    }
}
```

Collections and Maps don't have this issue because Carpet knows how is the behaviour of both types.

### Projections

One of key features of Parquet is that you can save a lot of I/O and CPU if you read only a subset of columns.

Given a parquet file, you can read a subset of columns just using a Record with needed columns.

For example, from a file with this schema, you can read just id, sku, and quantity fields:

```
message Invoice {
  optional binary id (STRING);
  required double amount;
  required double taxes;
  optional group lines (LIST) {
    repeated group list {
      optional group element {
        optional binary sku (STRING);
        required int32 quantity;
        required double price;
      }
    }
  }
}
```

defining this records:

```java
record LineRead(String sku, int quantity) { }

record InvoiceRead(String id, List<LineRead> lines) { }

List<InvoiceRead> data = new CarpetReader<>(new File("my_file.parquet"), InvoiceRead.class).toList();
```


### Nullability

Parquet supports to configure not null columns in the schema. Carpet, writing the schema, respects nullability of Java primitives.

This record:

```java
record MyRecord(long id, String name, int size, double value){ }
```

generates this schema with primitive types as `required`:

```java
message MyRecord {
  required int64 id;
  optional binary name (STRING);
  required int32 size;
  required double value;
}
```

while this record:

```java
record MyRecord(Long id, String name, Integer size, Double value) { }
```

generates this schema with all numeric values as `optional`:

```java
message MyRecord {
  optional int64 id;
  optional binary name (STRING);
  optional int32 size;
  optional double value;
}
```

String, List or Map types are objects and can be nullable. To generate a schema where an object reference field is created as `required` you must annotate the field with `@NotNull` annotation.

```java
record MyRecord(@NotNull String id, @NotNull String name, @NotNull Address address){ }
```

generates this schema:

```
message MyRecord {
  required binary id (STRING);
  required binary name (STRING);
  required group address {
    optional binary street (STRING);
    optional binary zip (STRING);
    optional binary city (STRING);
  }
}
```

### Read schema mismatch

How does Carpet behave when the schema does not exactly match records types?

#### Nullable column mapped to primitive type

By default Carpet doesn't fail when a column is defined as `optional` but the record field is primitive.

This parquet schema:

```
message MyRecord {
  required binary id (STRING);
  required binary name (STRING);
  optional int32 age;
}
```

is *compatible* with this record:

```java
record MyRecord(String id, String name, int age) { }
```

When a null value appears in a file, the field is filled with the [default value of the primitive](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html) (0, 0.0 or false).

If you want to ensure that the application fails if an optional column is mapped to a primitive field, you can enable the flag `FailOnNullForPrimitives`:

```java
List<MyRecord> data = new CarpetReader<>(file, MyRecord.class)
    .withFailOnNullForPrimitives(true)
    .toList();
```

By default, `FailOnNullForPrimitives` value is false.

#### Missing fields

When parquet file schema doesn't match with used record fields, Carpet throws an exception.

This schema:

```
message MyRecord {
  required binary id (STRING);
  required binary name (STRING);
}
```

is not compatible with this record because it contains an additional `int age` field:

```java
record MyRecord(String id, String name, int age) { }
```

If for some reason you are forced to read the file with an *incompatible* record, you can disable the schema compatibility check with flag `FailOnMissingColumn`:

```java
List<MyRecord> data = new CarpetReader<>(file, MyRecord.class)
    .withFailOnMissingColumn(false)
    .toList();
```

Carpet will skip the schema verification and fill the value with `null` in case of Objects or the [default value of primitives](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html) (0, 0.0 or false).

By default, `FailOnMissingColumn` value is true.

#### Narrowing numeric values

By default Carpet converts between numeric types:
* Any integer type can be converted to another integer type of different size: byte <-> short <-> int <-> long.
* Any decimal type can be converted to another decimal type of different size: float <-> double

This schema

```
message MyRecord {
  required int64 id;
  required double value;
}
```

is compatible with this record:

```java
record MyRecord(int id, float value) { }
```

Carpet will cast numeric types using [Narrowing Primitive Conversion](https://docs.oracle.com/javase/specs/jls/se10/html/jls-5.html) rules from Java.

If you want to ensure that the application fails if a type is converted to a narrow value, you can enable the flag `FailNarrowingPrimitiveConversion`:

```java
List<MyRecord> data = new CarpetReader<>(file, MyRecord.class)
    .withFailNarrowingPrimitiveConversion(true)
    .toList();
```

By default, `FailNarrowingPrimitiveConversion` value is false.

### Parquet configuration tunning

Default `CarpetWriter` constructors cover default `ParquetWriter` configuration. You can customize Parquet configuration using `CarpetWriter.Builder`, that exposes all configuration methods if you need to tune it (compression, sizes, hadoop usage, etc).

```java
List<MyRecord> data = calculateDataToPersist();

try (OutputStream outputStream = new FileOutputStream("my_file.parquet")) {
    try (ParquetWriter<MyRecord> writer = CarpetWriter.builder(outputStream, MyRecord.class)
        .withWriteMode(Mode.OVERWRITE)
        .withCompressionCodec(CompressionCodecName.GZIP)
        .withPageRowCountLimit(100_000)
        .withBloomFilterEnabled("name", true)
        .build()) {
    writer.write(data);
}
```

### Column name conversion

Default column name mapping uses Java attribute names as Parquet column names. You can modify this behaviour while configuring Carpet.

#### Writing

Writing a file, configure the property `columnNamingStrategy`:

```java
record MyRecord(long userCode, String userName){ }

List<MyRecord> data = calculateDataToPersist();
try (var writer = CarpetWriter.builder(outputStream, MyRecord.class)
    .withColumnNamingStrategy(ColumnNamingStrategy.SNAKE_CASE)
    .build()) {
  writer.write(data);
}
```

Creates a Parquet file with all column names converted to snake_case:

```
message MyRecord {
  required int64 user_code;
  optional binary user_name (STRING);
}
```

At the moment, only to snake conversion strategy is implemented.

#### Reading

To read a file using the inverse logic we must configure the property `fieldMatchingStrategy`:

```java
var reader = new CarpetReader<>(input, SomeEntity.class)
    .withFieldMatchingStrategy(FieldMatchingStrategy.SNAKE_CASE);
List<SomeEntity> list = reader.toList();
```

* The strategy `FIELD_NAME` aims to match a column with a field of the same name
* The strategy `SNAKE_CASE` aims to match a column name with a record field name converted to snake_case
* The strategy `BEST_EFFORT` first aims to match a column with a field of the same name. If no match is found, it then tries to find a field whose name converted to snake_case matches the column

### Low level Parquet classes

Carpet is built on top of [parquet-mr](https://github.com/apache/parquet-mr/) library and supports creating a `ParquetWriter` and `ParquetReader`, and use it with third party libraries that work with parquet classes.

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
}
```

### Local file system files

`parquet-mr` defines `OutputFile` and `InputFile` interfaces, but it only provides `HadoopOutputFile` and `HadoopInputFile` implementations in `parquet-hadoop` library. These implementations can access to files located in Hadoop and local file system.

Recently, Parquet main project has [merged to master](https://github.com/apache/parquet-mr/pull/1111) non Hadoop implementations that don't require to add Hadoop dependencies. This code has not been released to a stable version.

In the meantime, Carpet provides one local file implementation:

```java
InputFile inputFile = new FileSystemOutputFile(new File("my_file.parquet"));

InputFile outputFile = new FileSystemInputFile(new File("my_file.parquet"));
```

## Build

To run the unit tests:

```bash
./gradlew test
```

To build the jars:

```bash
./gradlew assemble
```

The build runs in [GitHub Actions](https://github.com/jerolba/parquet-carpet/actions):

[![Build Status](https://github.com/jerolba/parquet-carpet/actions/workflows/build-gradle-project.yml/badge.svg)](https://github.com/jerolba/parquet-carpet/actions)

## Contribute
Feel free to dive in! [Open an issue](https://github.com/jerolba/parquet-carpet/issues/new) or submit PRs.

Any contributor and maintainer of this project follows the [Contributor Covenant Code of Conduct](https://github.com/jerolba/parquet-carpet/blob/master/CODE_OF_CONDUCT.md).

## License
[Apache 2](https://github.com/jerolba/parquet-carpet/blob/master/LICENSE.txt) © Jerónimo López


