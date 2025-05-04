# Projections

One of key features of Parquet is that you can save a lot of I/O and CPU if you read only a subset of available columns in a file.

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

Parquet will read and parse only pages with id, sku, and quantity columns, skipping the rest of the file.