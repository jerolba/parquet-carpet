# Java Type Annotations

Carpet allows you to store `String`, Enum, or `org.apache.parquet.io.api.Binary` fields as the Parquet `BINARY` type with different logical types, such as String, Enum, JSON, or BSON. This is useful for embedding JSON, BSON documents, or any raw binary data directly into your Parquet files if no native type is available for your use case.

You can use the `@ParquetString`, `@ParquetEnum`, `@ParquetJson`, or `@ParquetBson` annotations to configure the logical type in the Parquet schema. These annotations do not transform or convert the actual data. They simply specify how the data should be interpreted in the Parquet format. Carpet does not validate the content of the data, so you must ensure that the data you are writing is valid String, JSON, or BSON.

These annotations can be applied to record components or collection elements (`List<@ParquetBson Binary> values`). The following sections describe how to use these annotations with different types.

## @ParquetString annotation

The `@ParquetString` annotation is used to specify that a field should be stored as a Parquet string type when the field type is an Enum or a Parquet Java `Binary` type.

By default, Carpet converts `Binary` and Enum types to their corresponding Parquet types. However, for some use cases, you may want to store them as binary strings instead, overriding the default behavior.

### With Binary type

The following record:

```java
record Person(String name, @ParquetString Binary code) { }
```

will be converted to the following Parquet schema:

```
message Person {
  optional binary name (STRING);
  optional binary code (STRING);
}
```

This is useful when the source of your information is a `Binary` type, but you still want to store it as a string in Parquet.

### With Enum type

The following record:

```java
enum Category { HIGH, MEDIUM, LOW }

record Person(String name, @ParquetString Category category) { }
```

will be converted to the following Parquet schema:

```
message Person {
  optional binary name (STRING);
  optional binary category (STRING);
}
```

You can work with enumerations while keeping their String representation in Parquet, without breaking contracts with other systems.

## @ParquetEnum annotation

The `@ParquetEnum` annotation is used to specify that a field should be stored as a Parquet enum type when the field type is a `String` or a Parquet Java `Binary` type.

By default, Carpet converts `Binary` and `String` types to their corresponding Parquet types. However, for some use cases, you may want to store them as binary Enum instead, overriding the default behavior.

### With Binary type

The following record:

```java
record Person(String name, @ParquetEnum Binary code) { }
```

will be converted to the following Parquet schema:

```
message Person {
  optional binary name (STRING);
  optional binary code (ENUM);
}
```

This is useful when the source of your information is a `Binary` type, but you still want to store it as an Enum in Parquet.

### With String type

The following record:

```java

record Person(String name, @ParquetEnum String category) { }
```

will be converted to the following Parquet schema:

```
message Person {
  optional binary name (STRING);
  optional binary category (ENUM);
}
```

You can work with Strings while keeping their Enum representation in Parquet, without breaking contracts with other systems.

## @ParquetJson annotation

Java does not have a native JSON type, but you can use `String` or `Binary` to store JSON data. The `@ParquetJson` annotation is used to specify that a field should be stored as a Parquet JSON type when the field type is a `String` or `Binary`.

To store a field as JSON, annotate the record component with `@ParquetJson`. The data will be stored as Parquet `binary` with the `JSON` logical type.

The following record:

```java
record ProductEvent(long id, Instant timestamp, @ParquetJson String jsonData){}
```

generates a schema with a `binary` field annotated with the `JSON` logical type:

```
message ProductEvent {
    required int64 id;
    required int64 timestamp (TIMESTAMP(MILLIS,true));
    optional binary jsonData (JSON);
}
```

`@ParquetJson` can also annotate the `Binary` class.

## @ParquetBson annotation

Similar to JSON, Java does not have a native BSON type, but you can use the `Binary` type to store BSON data. The `@ParquetBson` annotation is used to specify that a field should be stored as a Parquet BSON type when the field type is `Binary`.

The following record:

```java
record ProductEvent(long id, Instant timestamp, @ParquetBson Binary bsonData){}
```

generates a schema with a `binary` field annotated with the `BSON` logical type:

```
message ProductEvent {
    required int64 id;
    required int64 timestamp (TIMESTAMP(MILLIS,true));
    optional binary bsonData (BSON);
}
```

Carpet does not validate the content of the data, so you must ensure that the data you are writing is valid BSON.

## BigDecimal type

The `BigDecimal` type is used to represent arbitrary-precision decimal numbers. In Parquet, `BigDecimal` can be represented by multiple physical Parquet types, all configured with the `DECIMAL` logical type and a specified precision and scale.

### @PrecisionScale annotation

The precision is the total number of digits, and the scale is the number of digits to the right of the decimal point.

When writing a file, the precision and scale can be configured globally in the [writer configuration](../configuration/#bigdecimal-precision-and-scale) or per record field using the `@PrecisionScale` annotation. Annotation configuration takes precedence over the writer configuration.

The following record:

```java
record Product(long id, @PrecisionScale(20, 4) BigDecimal price) {}
```

will be converted to the following Parquet schema:

```
message Product {
  required int64 id;
  optional binary price (DECIMAL(20,4));
}
```

When writing a file with a configured precision and scale, Carpet adapts the data to these specifications. If the data in the file has a different precision or scale, it will be converted to the specified precision and scale.

When reading a file with a record field annotated with `@PrecisionScale`, Carpet does NOT validate the precision and scale of the data. It reads the data as `BigDecimal` using the precision and scale from the file. If the data in the file has a different precision or scale, Carpet will not throw an error or convert it. You must ensure that the data you are reading is valid for the specified precision and scale.

### @Rounding annotation

If scale adjustment is needed, you must configure the rounding mode to round the value to the specified scale.

When writing a file, the rounding mode can be configured globally in the [writer configuration](../configuration/#bigdecimal-precision-and-scale) or per record field using the `@Rounding` annotation. Annotation configuration takes precedence over the writer configuration.

The `@Rounding` annotation requires a `RoundingMode` enum parameter, which is used to round `BigDecimal` values in the Java API. This annotation does not modify the generated Parquet schema but configures the rounding mode for `BigDecimal` values.

```java
record Product(
    long id,
    @PrecisionScale(20, 4) @Rounding(RoundingMode.HALF_UP) BigDecimal price) {
}
```

If the rounding mode is not specified via annotation or writer configuration, the default is `RoundingMode.UNNECESSARY`. This means an exception will be thrown if rounding is necessary, which is useful to ensure data integrity if no changes are expected during conversion.

`@PrecisionScale` and `@Rounding` annotations can be used together or separately, depending on your use case and how you want to configure the precision and scale of `BigDecimal` values in your Parquet files.