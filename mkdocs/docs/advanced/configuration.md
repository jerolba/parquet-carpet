# Advanced Configuration

Parquet library provides multiple configuration options to customize the behavior of the writer and reader. `CarpetWriter` and `CarpetReader` hide most of these options, but you can still access them if needed.

On the other hand, `Carpet` requires some optional configuration to setup how to handle specific types, such as `BigDecimal` and `LocalDateTime`.

This section will cover the advanced configurations available in Carpet.

## Writer Configuration

### Parquet Configuration

Default `CarpetWriter` constructors cover default `ParquetWriter` configuration. You can customize Parquet configuration using `CarpetWriter.Builder`, that exposes all configuration methods if you need to tune it (compression, sizes, hadoop usage, etc).

```java
List<MyRecord> data = calculateDataToPersist();

try (CarpetWriter<MyRecord> writer = new CarpetWriter.Builder<>(outputFile, MyRecord.class)
    .withWriteMode(Mode.OVERWRITE)
    .withCompressionCodec(CompressionCodecName.GZIP)
    .withPageRowCountLimit(100_000)
    .withBloomFilterEnabled("name", true)
    .build()) {
writer.write(data);
```

Any `ParquetWriter` configuration can be set using the `CarpetWriter.Builder`.

### Carpet Configuration

Carpet provides some global configuration options to customize the default behavior of the writer managing some types.

#### BigDecimal precision and scale

[DECIMAL](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal) type requires specifying both precision and scale when persisting values. This configuration is set globally when writing a file:

```java

record MyRecord(String id, String name, BigDecimal price) { }

try (var writer = new CarpetWriter.Builder<>(outputFile, MyRecord.class)
        .withDefaultDecimal(precision, scale)
        .build()) {
```

There is no default value. If `BigDecimal` type is encountered, but precision and scale are not configured, Carpet throws an exception.

If a `BigDecimal` value has a higher scale than the configured scale, **Carpet does not rescale it by default** and instead it throws an exception. To prevent this and automatically rescale values to the configured scale, you must specify the `RoundingMode` using the `withBigDecimalScaleAdjustment` method:

```java
try (var writer = new CarpetWriter.Builder<>(outputFile, MyRecord.class)
        .withDefaultDecimal(20, 3)
        .withBigDecimalScaleAdjustment(RoundingMode.HALF_UP)
        .build()) {
    writer.write(new MyRecord("1", "item1", new BigDecimal("123.45678")));
```

This configuration is only applied when writing the file. When reading, the `BigDecimal` values are read as they are stored in the file, without any adjustment.

#### Time-Unit Configuration

[TIME](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#time) and [TIMESTAMP](https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp) Parquet types support configuring the decimal second unit (`MILLIS`, `MICROS` or `NANOS`).

In Carpet, Time-Unit configuration is global when writing a file, and by default it's configured as `MILLIS`.

The global configuration can be overwritten in the CarpetWriter builder:

```java
record MyRecord(long itemId, int count, LocalTime saleTime) { }

var writer = new CarpetWriter.Builder<>(outputStream, MyRecord.class)
        .withDefaultTimeUnit(TimeUnit.MICROS);
        .build()) {
```

This configuration is only applied when writing the file. When reading, the `LocalTime`, `LocalDateTime` and `Instant` values are read as they are stored in the file, without any adjustment.

## Reader Configuration

### Parquet Configuration

CarpetReader doesn't provide a builder. It has been simplified to just provide Carpet specific configuration. You can still access all `ParquetReader` configuration options using the `CarpetParquetReader.Builder`.

### Carpet Configuration

CarpetReader provides some configuration options to customize the behavior of the reader matching the schema of the file with the schema of the class used to read it.

Configure how schema mismatches are handled:

```java
var reader = new CarpetReader<>(file, MyRecord.class)
    // Fail on null values for primitives
    .withFailOnNullForPrimitives(true)
    // Allow missing columns in the file
    .withFailOnMissingColumn(false)
    // Prevent narrowing conversions
    .withFailNarrowingPrimitiveConversion(true)
    // Flexible name matching
    .withFieldMatchingStrategy(FieldMatchingStrategy.BEST_EFFORT);
```