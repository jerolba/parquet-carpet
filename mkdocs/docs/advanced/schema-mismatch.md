# Read Schema mismatch

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

If you want to ensure that the application fails if an optional column is mapped to a primitive field, you must enable the flag `FailOnNullForPrimitives`:

```java
List<MyRecord> data = new CarpetReader<>(file, MyRecord.class)
    .withFailOnNullForPrimitives(true)
    .toList();
```

By default, `FailOnNullForPrimitives` value is false.

#### Missing fields

When parquet file schema doesn't match with existing record fields, Carpet throws an exception.

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

If a column that exists in the file is not present in the record, Carpet will ignore it and will not throw an exception because it's considered a [projection](./projections.md).

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

Carpet will **cast numeric types** using [Narrowing Primitive Conversion](https://docs.oracle.com/javase/specs/jls/se10/html/jls-5.html) rules from Java.

If you want to ensure that the application fails if a type is converted to a narrow value, you can enable the flag `FailNarrowingPrimitiveConversion`:

```java
List<MyRecord> data = new CarpetReader<>(file, MyRecord.class)
    .withFailNarrowingPrimitiveConversion(true)
    .toList();
```

By default, `FailNarrowingPrimitiveConversion` value is false.