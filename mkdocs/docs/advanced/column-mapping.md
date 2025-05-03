# Column Name Mapping

Carpet uses reflection to discover the schema of your files. Since Java record attribute names are limited by Java syntax while Parquet column names are more flexible.


## Column Name Aliasing

You can use the `@Alias` annotation to specify a different name for a field in the Parquet schema. This is useful when you want to map a Java field to a Parquet column with a different name or format.

```java
record MyRecord(long id, String name, @Alias("$name.id") String nameId) { }
```

## Column Name Conversion

Carpet supports automatic conversion of Java field names to Parquet column names. By default, it uses the same name as the field. However, you can modify this behaviour while configuring Carpet.

### Writing

Writing a file, configure the property `columnNamingStrategy`:

```java
record MyRecord(long userCode, String userName) { }

List<MyRecord> data = calculateDataToPersist();
try (var writer = new CarpetWriter.Builder<>(outputStream, MyRecord.class)
    .withColumnNamingStrategy(ColumnNamingStrategy.SNAKE_CASE)
    .build()) {
  writer.write(data);
}
```

This will create a Parquet file with columns named `user_code` and `user_name`:

```
message MyRecord {
  required int64 user_code;
  optional binary user_name (STRING);
}
```

At the moment, only the snake conversion strategy is implemented.

### Reading

To read a file using the inverse logic we must configure the property `fieldMatchingStrategy`:

```java
var reader = new CarpetReader<>(input, SomeEntity.class)
    .withFieldMatchingStrategy(FieldMatchingStrategy.SNAKE_CASE);
List<SomeEntity> list = reader.toList();
```

Available strategies reading a file are:

- `FIELD_NAME`: Match column with exact field name
- `SNAKE_CASE`: Match column with snake_case version of field name
- `BEST_EFFORT`: Try exact match first, then try snake_case

Reading and writing a file, @Alias annotation has precedence over the strategy configuration.