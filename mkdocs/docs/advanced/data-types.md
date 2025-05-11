# Supported Data Types

## Basic Types

Carpet maps main Java types to Parquet data types automatically:

| Java Type | Parquet Type |
|---|---|
| boolean/Boolean | boolean |
| byte/Byte | int32 |
| short/Short | int32 |
| int/Integer | int32 |
| long/Long | int64 |
| float/Float | float |
| double/Double | double |
| Binary | binary |
| String | binary (STRING) |
| Enum | binary (ENUM) |
| UUID | fixed_len_byte_array(16) (UUID) |

## Temporal Types

Support for Java time types:

| Java Type | Parquet Type |
|---|---|
| LocalDate | int32 (DATE) |
| LocalTime | int32 (TIME(MILLIS\|MICROS)) or int64 (TIME(NANOS)) |
| LocalDateTime | int64 (TIMESTAMP(MILLIS\|MICROS\|NANOS)) |
| Instant | int64 (TIMESTAMP(MILLIS\|MICROS\|NANOS)) |

## Decimal Numbers

BigDecimal mapping depends on precision:

| Precision | Parquet Type |
|---|---|
| ≤ 9 | int32 (DECIMAL) |
| ≤ 18 | int64 (DECIMAL) |
| > 18 | binary (DECIMAL) or fixed_len_byte_array (DECIMAL) |

## Binary

Carpet supports storing binary data using the `org.apache.parquet.io.api.Binary` class. This is useful for storing raw binary data that doesn't fit into other types. The `Binary` class provides methods for creating and manipulating binary data.

Following record:

```java
record SimpleRecord(long id, Binary data) { }
```

generates a Parquet schema with a `binary` type:

```
message SimpleRecord {
    required int64 id;
    optional binary data;
}
```

## JSON and BSON types

Java doesn't have a native JSON or BSON type, but you can use `String` or `org.apache.parquet.io.api.Binary` to store JSON or BSON data.

To configure it, you can use the `@ParquetJson` or `@ParquetBson` annotations to specify the logical type in Parquet schema.

You can find more information about JSON and BSON in the [Java Type Annotations](../java-type-annotations/) section.

## Nested Structures

Parquet supports nested structures, and Carpet can generate them using Java records and Collections.

Carpet uses the following rules to generate nested structures:

1. Types must be concrete and cannot be generic.
2. Types cannot be recursive directly or indirectly.

### Nested Records

Carpet supports nested records to create files with structured data. There is one exception: types can not be recursive directly nor indirectly.

```java
record Address(String street, String zip, String city) { }
record Job(String company, String position, int years) { }
record Person(long id, Job job, Address address) { }

try (var writer = new CarpetWriter<>(outputFile, Person.class)) {
    var president = new Person(1010101, new Job("USA", POTUS, 3),
        new Address("1600 Pennsylvania Av.", "20500", "Washington"));
    writer.write(president));
}
```

The generated file has this Parquet schema:

```
message Person {
  required int64 id;
  optional group job {
    optional binary company (STRING);
    optional binary position (STRING);
    required int32 years;
  }
  optional group address {
    optional binary street (STRING);
    optional binary zip (STRING);
    optional binary city (STRING);
  }
}
```

### Collections

Carpet supports nested collections to create files with structured data. Collection elements must be one of the supported types.

```java

record Line(String sku, int quantity, double price){ }
record Invoice(String id, double amount, double taxes, List<Line> lines) { }

try (var writer = new CarpetWriter<>(outputFile, Invoice.class)) {
    var invoice = new Invoice("2023/211", 2323.23, 232.32, List.of(
        new Line("AAA", 3, 500.0), new Line("BBB", 1, 823.23)));
    writer.write(invoice);
}
```

The generated file has this Parquet schema:

```
message Invoice {
  optional binary id (STRING);
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

### Maps

Carpet supports nested maps to create files with structured data. Map elements must be one of the supported types.

```java
record State(double area, int population){ }
record Country(String name, double area, Map<String, State> states) { }

try (var writer = new CarpetWriter<>(outputFile, Country.class)) {
    var country = new Country("USA", 9_833_520.0, Map.of(
        "Idaho", new State(216_444.0, 1_975_000),
        "Texas", new State(695_662.0, 29_145_505)));
    writer.write(country);
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

## Limitations

### Generic Types

Records cannot have generic elements as Carpet needs concrete type information to create the Parquet schema. This includes generic records, collections, and maps.

```java
// This will NOT work
record WithGeneric<T>(String name, T child) { }

// This works fine
record WithList(String name, List<String> items) { }
record WithMap(String name, Map<String, Integer> values) { }
```

If generic type is used, `RecordTypeConversionException` will be thrown.

Collections and Maps with concrete types don't have this issue because Carpet knows their concrete type information at compile time.

### Recursive Types

Records cannot have direct or indirect recursive references:

```java
// This will NOT work - direct recursion
record Node(String id, Node next) { }

// This will NOT work - indirect recursion
record Child(String id, Parent parent) { }
record Parent(String id, Child child) { }
```
