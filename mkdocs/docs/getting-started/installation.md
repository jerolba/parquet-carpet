# Installation

## Maven

Include Carpet in your Java project using Maven by adding this dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>com.jerolba</groupId>
    <artifactId>carpet-record</artifactId>
    <version>0.4.0</version>
</dependency>
```

## Gradle

If you're using Gradle, add this to your `build.gradle`:

```gradle
implementation 'com.jerolba:carpet-record:0.4.0'
```

## Transitive dependencies and Hadoop

Carpet is designed to work with local filesystems by default and includes only the minimal Parquet-related dependencies needed for read/write operations.

While Parquet was originally developed as part of the Hadoop ecosystem, Carpet explicitly excludes most Hadoop-related transitive dependencies to keep the library lightweight.

If you need Hadoop functionality (like HDFS support), you'll need to explicitly add Hadoop dependencies to your project.

