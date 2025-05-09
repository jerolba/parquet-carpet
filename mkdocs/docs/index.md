# Carpet: Parquet Serialization and Deserialization Library for Java

[![Build Status](https://github.com/jerolba/parquet-carpet/actions/workflows/build-gradle-project.yml/badge.svg)](https://github.com/jerolba/parquet-carpet/actions)
[![Maven Central](https://img.shields.io/maven-central/v/com.jerolba/carpet-record.svg)](https://maven-badges.herokuapp.com/maven-central/com.jerolba/carpet-record)
[![License](http://img.shields.io/:license-apache-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![javadoc](https://javadoc.io/badge2/com.jerolba/carpet-record/javadoc.svg)](https://javadoc.io/doc/com.jerolba/carpet-record)
[![codecov](https://codecov.io/gh/jerolba/parquet-carpet/graph/badge.svg?token=zE0Xqe7fky)](https://codecov.io/gh/jerolba/parquet-carpet)

A Java library for serializing and deserializing Parquet files efficiently using Java records. This library provides a simple and user-friendly API for working with Parquet files, making it easy to read and write data in the Parquet format in your Java applications.

## Features

- Serialize Java records to Parquet files
- Deserialize Parquet files to Java records
- Support nested data structures
- Support nested Collections and Maps
- Very simple API
- Low level configuration of Parquet properties
- Low overhead processing files
- Minimized `parquet-java` and hadoop transitive dependencies

## Quick Start

Add the dependency to your project:

=== "Maven"

    ```xml
    <dependency>
        <groupId>com.jerolba</groupId>
        <artifactId>carpet-record</artifactId>
        <version>0.3.0</version>
    </dependency>
    ```

=== "Gradle"

    ```xml
    implementation 'com.jerolba:carpet-record:0.3.0'
    ```

Write and read your data:

```java
// Define your data structure
record MyRecord(long id, String name, int size, double value) { }

// Write to Parquet
List<MyRecord> data = calculateDataToPersist();
try (var outputStream = new FileOutputStream("my_file.parquet")) {
    try (var writer = new CarpetWriter<>(outputStream, MyRecord.class)) {
        writer.write(data);
    }
}

// Read from Parquet
List<MyRecord> data = new CarpetReader<>(new File("my_file.parquet"), MyRecord.class).toList();
```

Check out the [Getting Started](getting-started/installation.md) guide for more details.
