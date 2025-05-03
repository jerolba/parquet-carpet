# Nullability

Parquet supports to configure not null columns in the schema. Carpet, writing the schema, respects Java primitives' nullability:

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

The `@NotNull` annotation is not part of the Java standard library and Carpet provides one implementation. You can use any library that provides this type of annotation, such as `javax.validation.constraints.NotNull` or `jakarta.annotation.Nonnull`. Carpet inspects fields annotation looking by the name of the annotation not the complete type.