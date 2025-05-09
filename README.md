[![Build Status](https://github.com/jerolba/parquet-carpet/actions/workflows/build-gradle-project.yml/badge.svg)](https://github.com/jerolba/parquet-carpet/actions)
[![Maven Central](https://img.shields.io/maven-central/v/com.jerolba/carpet-record.svg)](https://maven-badges.herokuapp.com/maven-central/com.jerolba/carpet-record)
[![License](http://img.shields.io/:license-apache-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![javadoc](https://javadoc.io/badge2/com.jerolba/carpet-record/javadoc.svg)](https://javadoc.io/doc/com.jerolba/carpet-record)
[![codecov](https://codecov.io/gh/jerolba/parquet-carpet/graph/badge.svg?token=zE0Xqe7fky)](https://codecov.io/gh/jerolba/parquet-carpet)

# Carpet: Parquet Serialization and Deserialization Library for Java

A Java library for serializing and deserializing Parquet files efficiently using Java records. This library provides a simple and user-friendly API for working with Parquet files, making it easy to read and write data in the Parquet format in your Java applications.

**For comprehensive documentation, please visit our [full documentation site](https://carpet.jerolba.com/).**

## Features

- Serialize Java records to Parquet files
- Deserialize Parquet files to Java records
- Support nested data structures
- Support nested Collections and Maps
- Very simple API
- Low level configuration of Parquet properties
- Low overhead procesing files
- Minimized `parquet-java` and hadoop transitive dependencies


## Table of Contents

- [Installation](#installation)
- [Basic Usage](#basic-usage)
- [Advanced Usage (Overview)](#advanced-usage)
- [Full Documentation Site](https://carpet.jerolba.com/)
- [Contribute](#contribute)
- [Build](#build)
- [License](#license)

## Installation

You can include this library in your Java project using Maven:

```xml
<dependency>
    <groupId>com.jerolba</groupId>
    <artifactId>carpet-record</artifactId>
    <version>0.3.0</version>
</dependency>
```

or using Gradle:

```gradle
implementation 'com.jerolba:carpet-record:0.3.0'
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

Carpet offers a rich set of features for advanced scenarios. For detailed explanations, API references, and examples, please refer to our [comprehensive documentation site](https://carpet.jerolba.com/).

Key advanced topics include:

- **API Details**:
    - [CarpetWriter API](https://carpet.jerolba.com/advanced/configuration/) <!-- Assuming writer API details are part of general configuration or a specific page -->
    - [CarpetReader API](https://carpet.jerolba.com/advanced/configuration/) <!-- Assuming reader API details are part of general configuration or a specific page -->
- **Schema and Data Handling**:
    - [Column Name Mapping & Conversion](https://carpet.jerolba.com/advanced/column-mapping/)
    - [Supported Data Types (including nested structures, collections, maps)](https://carpet.jerolba.com/advanced/data-types/)
    - [Projections](https://carpet.jerolba.com/advanced/projections/)
    - [Nullability](https://carpet.jerolba.com/advanced/nullability/)
    - [Handling Read Schema Mismatches](https://carpet.jerolba.com/advanced/configuration/#read-schema-mismatch) <!-- Example: Link to a sub-section -->
- **Configuration & Low-Level Access**:
    - [Parquet Configuration Tuning (compression, page sizes, etc.)](https://carpet.jerolba.com/advanced/configuration/)
    - [BigDecimal Precision and Scale](https://carpet.jerolba.com/advanced/configuration/#bigdecimal-precision-and-scale)
    - [Time Unit Configuration](https://carpet.jerolba.com/advanced/configuration/#time-unit-configuration)
    - [Low-Level Parquet Classes Integration](https://carpet.jerolba.com/advanced/low-level-parquet/)
    - [Local File System File Handling](https://carpet.jerolba.com/advanced/input-output-files/)

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


