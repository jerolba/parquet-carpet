# Write Model

The Write Model feature in Carpet allows you to customize how your Java objects are written to Parquet files. While Carpet can automatically map Java Records to Parquet schemas using reflection, the Write Model provides fine-grained control over field mapping, data types, and serialization behavior.

## What is a Write Model?

A Write Model is a programmatic specification that defines:

- Which fields from your Java objects should be written to the Parquet file
- How those fields should be named in the Parquet schema
- What Parquet data types should be used for each field
- How to extract values from your objects (accessor functions)

This is particularly useful when:

- You want to write only specific fields from your objects
- You need to rename fields in the Parquet file
- You're working with classes that aren't records
- You want to customize data types or logical types
- You need to write data in a specific format for compatibility

## Understanding Parquet Context

Parquet is a columnar storage format that requires a predefined schema. When writing data to Parquet:

1. **Schema Definition**: The structure must be known before writing any data
2. **Data Types**: Each column has a specific primitive type (INT32, INT64, BINARY, etc.)
3. **Logical Types**: Additional metadata that defines how to interpret primitive types
4. **Repetition**: Fields can be required, optional, or repeated (for collections)

Carpet's Write Model bridges the gap between your Java objects and Parquet's requirements, giving you control over this mapping process.

## Basic Usage

### Creating a Write Model

Use the `writeRecordModel()` factory method to create a Write Model for your class:

```java
import static com.jerolba.carpet.model.FieldTypes.*;

// Define your data class
record Person(String name, int age, String email) {}

// Create a Write Model
var writeModel = writeRecordModel(Person.class)
    .withField("person_name", STRING, Person::name)
    .withField("person_age", INTEGER, Person::age)
    .withField("email_address", STRING, Person::email);
```

### Using the Write Model

Apply the Write Model when creating a Parquet writer:

```java
// Using CarpetWriter
try (CarpetWriter<Person> writer = new CarpetWriter.Builder<>(outputStream, Person.class)
        .withWriteRecordModel(writeModel)
        .build()) {
    writer.write(List.of(
        new Person("Alice", 30, "alice@example.com"),
        new Person("Bob", 25, "bob@example.com")
    ));
}

// Using CarpetParquetWriter
try (ParquetWriter<Person> writer = CarpetParquetWriter.builder(outputFile, Person.class)
        .withWriteRecordModel(writeModel)
        .build()) {
    writer.write(new Person("Charlie", 35, "charlie@example.com"));
}
```

## Field Mapping

### Basic Field Types

Map fields using primitive type accessors and field type constants:

```java
record Product(int id, String name, Double price, boolean available) {}

var writeModel = writeRecordModel(Product.class)
    .withField("product_id", Product::id)           // Primitive int
    .withField("name", STRING, Product::name)       // String with type
    .withField("price", DOUBLE, Product::price)     // Double object
    .withField("in_stock", Product::available);     // Primitive boolean
```

### Primitive Type Methods

For primitive types, you can use specialized accessor methods that avoid boxing and specify the field type implicitly:

```java
record Metrics(int count, long total, float average, double ratio,
               short level, byte flag, boolean active) {}

var writeModel = writeRecordModel(Metrics.class)
    .withField("count", Metrics::count)         // ToIntFunction
    .withField("total", Metrics::total)         // ToLongFunction
    .withField("average", Metrics::average)     // ToFloatFunction
    .withField("ratio", Metrics::ratio)         // ToDoubleFunction
    .withField("level", Metrics::level)         // ToShortFunction
    .withField("flag", Metrics::flag)           // ToByteFunction
    .withField("active", Metrics::active);      // ToBooleanFunction
```

### Object Field Types

For object types, you need to specify both the field type and accessor function:

```java
record Order(UUID id, String customerName, BigDecimal amount,
             LocalDate orderDate, LocalDateTime createdAt) {}

var writeModel = writeRecordModel(Order.class)
    .withField("order_id", UUID, Order::id)
    .withField("customer", STRING, Order::customerName)
    .withField("amount", BIG_DECIMAL, Order::amount)
    .withField("order_date", LOCAL_DATE, Order::orderDate)
    .withField("created_at", LOCAL_DATE_TIME, Order::createdAt);
```

## Working with Collections

### Lists

Use `LIST.ofType()` to define list fields:

```java
record ShoppingCart(String userId, List<String> productIds, List<Integer> quantities) {}

var writeModel = writeRecordModel(ShoppingCart.class)
    .withField("user_id", STRING, ShoppingCart::userId)
    .withField("products", LIST.ofType(STRING), ShoppingCart::productIds)
    .withField("quantities", LIST.ofType(INTEGER), ShoppingCart::quantities);
```

### Sets

Sets are handled similarly to lists and are mapped to Parquet with the same type that lists use:

```java
record UserProfile(String username, Set<String> tags, Set<Category> categories) {}

var writeModel = writeRecordModel(UserProfile.class)
    .withField("username", STRING, UserProfile::username)
    .withField("tags", SET.ofType(STRING), UserProfile::tags)
    .withField("categories", SET.ofType(ENUM.ofType(Category.class)), UserProfile::categories);
```

### Maps

Use `MAP.ofTypes()` for key-value mappings. Maps require both key and value types:

```java
record Configuration(String appName, Map<String, String> settings,
                    Map<String, Integer> counters) {}

var writeModel = writeRecordModel(Configuration.class)
    .withField("app_name", STRING, Configuration::appName)
    .withField("settings", MAP.ofTypes(STRING, STRING), Configuration::settings)
    .withField("counters", MAP.ofTypes(STRING, INTEGER), Configuration::counters);
```

## Working with Nested Records

Write Models support nested structures by composing multiple `writeRecordModel` definitions. This allows you to define complex hierarchical data structures where one record contains another record as a field.

### Basic Nested Records

Define nested structures by using a `writeRecordModel` as a field type:

```java
record Address(String street, String city, String zipCode) {}
record Person(String name, int age, Address address) {}

// Define the nested record model first
var addressModel = writeRecordModel(Address.class)
    .withField("street", STRING, Address::street)
    .withField("city", STRING, Address::city)
    .withField("zip_code", STRING, Address::zipCode);

// Use the nested model in the parent record
var personModel = writeRecordModel(Person.class)
    .withField("full_name", STRING, Person::name)
    .withField("age", INTEGER, Person::age)
    .withField("address", addressModel, Person::address);
```

### Inline Nested Models

You can also define nested models inline for simpler cases:

```java
record Department(String name, String code) {}
record Employee(String name, Department department) {}

var employeeModel = writeRecordModel(Employee.class)
    .withField("employee_name", STRING, Employee::name)
    .withField("dept", writeRecordModel(Department.class)
        .withField("dept_name", STRING, Department::name)
        .withField("dept_code", STRING, Department::code), Employee::department);
```

### Multiple Level Nesting

Write Models support arbitrary levels of nesting:

```java
record Country(String name, String code) {}
record State(String name, Country country) {}
record City(String name, State state) {}
record Address(String street, City city) {}

// Build models from innermost to outermost
var countryModel = writeRecordModel(Country.class)
    .withField("name", STRING, Country::name)
    .withField("code", STRING, Country::code);

var stateModel = writeRecordModel(State.class)
    .withField("name", STRING, State::name)
    .withField("country", countryModel, State::country);

var cityModel = writeRecordModel(City.class)
    .withField("name", STRING, City::name)
    .withField("state", stateModel, City::state);

var addressModel = writeRecordModel(Address.class)
    .withField("street", STRING, Address::street)
    .withField("city", cityModel, Address::city);
```

### Handling Null Nested Objects

Nested records can be null, and the Write Model handles this gracefully:

```java
record ContactInfo(String email, String phone) {}
record Customer(String name, ContactInfo contact) {}

var customerModel = writeRecordModel(Customer.class)
    .withField("customer_name", STRING, Customer::name)
    .withField("contact", writeRecordModel(ContactInfo.class)
        .withField("email", STRING, ContactInfo::email)
        .withField("phone", STRING, ContactInfo::phone), Customer::contact);

// Usage with null nested object
var customer1 = new Customer("John Doe", new ContactInfo("john@example.com", "555-0123"));
var customer2 = new Customer("Jane Smith", null); // Null contact info
```

### Nested Records with Collections

Combine nested records with collections for complex data structures:

```java
record Item(String name, BigDecimal price) {}
record Order(String orderId, List<Item> items, Address shippingAddress) {}

var itemModel = writeRecordModel(Item.class)
    .withField("product_name", STRING, Item::name)
    .withField("unit_price", BIG_DECIMAL.withPrecisionScale(10, 2), Item::price);

var orderModel = writeRecordModel(Order.class)
    .withField("order_id", STRING, Order::orderId)
    .withField("items", LIST.ofType(itemModel), Order::items)
    .withField("shipping_address", addressModel, Order::shippingAddress);
```

Nested Write Model can also be used as keys or values in maps.

## Advanced Field Types

### Enums

Configure how enums are stored in Parquet:

```java
enum Priority { LOW, MEDIUM, HIGH }

record Task(String title, Priority priority) {}

// Store as enum logical type (default)
var writeModel1 = writeRecordModel(Task.class)
    .withField("title", STRING, Task::title)
    .withField("priority", ENUM.ofType(Priority.class), Task::priority);

// Store as string logical type
var writeModel2 = writeRecordModel(Task.class)
    .withField("title", STRING, Task::title)
    .withField("priority", ENUM.ofType(Priority.class).asString(), Task::priority);
```

### BigDecimal with Precision

Configure decimal precision and scale:

```java
record FinancialRecord(String account, BigDecimal balance) {}

var writeModel = writeRecordModel(FinancialRecord.class)
    .withField("account", STRING, FinancialRecord::account)
    .withField("balance", BIG_DECIMAL.withPrecisionScale(10, 2),
               FinancialRecord::balance);
```

### Date and Time Types

Carpet supports all Java time types with proper Parquet logical type mappings:

```java
record EventRecord(
    String eventId,
    LocalDate eventDate,        // Date only (year-month-day)
    LocalTime eventTime,        // Time only (hour-minute-second-nano)
    LocalDateTime timestamp,    // Date and time without timezone
    Instant utcTimestamp        // UTC timestamp with timezone
) {}

var writeModel = writeRecordModel(EventRecord.class)
    .withField("event_id", STRING, EventRecord::eventId)
    .withField("event_date", LOCAL_DATE, EventRecord::eventDate)
    .withField("event_time", LOCAL_TIME, EventRecord::eventTime)
    .withField("timestamp", LOCAL_DATE_TIME, EventRecord::timestamp)
    .withField("utc_timestamp", INSTANT, EventRecord::utcTimestamp);
```

The precision of time fields are globally configured in the CarpetWriter builder via `withDefaultTimeUnit` configuration method.

### Geospatial Data

Handle geometric data with specific coordinate systems and geospatial types:

```java
import org.locationtech.jts.geom.Geometry;

record Location(String name, Geometry geometry) {}

var geometryModel = writeRecordModel(Location.class)
    .withField("name", STRING, Location::name)
    .withField("geometry", GEOMETRY.asParquetGeometry("EPSG:4326"),
               Location::geometry);

var geographyModel = writeRecordModel(Location.class)
    .withField("name", STRING, Location::name)
    .withField("geometry", GEOMETRY.asParquetGeography  ("OGC:CRS84", EdgeInterpolationAlgorithm.SPHERICAL),
               Location::geometry);
```

### JSON, BSON and Binary Data

Handle JSON strings, BSON and Binary data with logical types:

```java
record Document(String title, String jsonData, Binary bsonData, Binary content) {}

var writeModel = writeRecordModel(Document.class)
    .withField("title", STRING, Document::title)
    .withField("json_data", STRING.asJson(), Document::jsonData)
    .withField("bson_data", BINARY.asBson(), Document::bsonData)
    .withField("content", BINARY, Document::content);
```

## Custom Field Extraction

You don't need to define new record classes for every use case. You can create custom fields or redefine existing ones using accessor functions.

### Computed Fields

Create fields based on calculations or transformations:

```java
record Employee(String firstName, String lastName, LocalDate birthDate) {}

var now = LocalDate.now();
var writeModel = writeRecordModel(Employee.class)
    .withField("first_name", STRING, Employee::firstName)
    .withField("last_name", STRING, Employee::lastName)
    .withField("full_name", STRING, emp -> emp.firstName() + " " + emp.lastName())
    .withField("birth_date", LOCAL_DATE, Employee::birthDate)
    .withField("age_years", INTEGER, emp ->
        Period.between(emp.birthDate(), now).getYears());
```

### Conditional Logic

Apply business logic during field extraction:

```java
record Order(String id, double amount, String status) {}

var writeModel = writeRecordModel(Order.class)
    .withField("order_id", STRING, Order::id)
    .withField("amount", DOUBLE, Order::amount)
    .withField("status", STRING, Order::status)
    .withField("is_large_order", BOOLEAN, order -> order.amount() > 1000.0)
    .withField("amount_category", STRING, order -> {
        if (order.amount() < 100) return "SMALL";
        if (order.amount() < 1000) return "MEDIUM";
        return "LARGE";
    });
```

### Dynamic fields creation

You can create fields dynamically based on runtime conditions or configurations:

Following example creates fields based on a list of valid item codes:

```java
record Item(String code, double value, String meta) {}
record Order(UUID orderId, Map<String, Item> items) {}

List<String> validCodes = List.of("a", "b", "c", "d"); // Could be loaded from config
var writeModel = writeRecordModel(Order.class)
    .withField("order_id", UUID, Order::orderId);
for (var code : validCodes) {
    writeModel.withField("item_" + code + "_value", DOUBLE, order -> order.items().get(code).value());
    writeModel.withField("item_" + code + "_meta", STRING, order -> order.items().get(code).meta());
}
```

## Field Type Customization

### Nullable vs Not-Null Fields

Control field nullability:

```java
record Data(String required, String optional) {}

var writeModel = writeRecordModel(Data.class)
    .withField("required_field", STRING.notNull(), Data::required)
    .withField("optional_field", STRING, Data::optional);
```

### Logical Types for Binary Fields

Customize how binary data is written in the schema:

```java
record BinaryData(Binary jsonData, Binary enumData, Binary stringData) {}

var writeModel = writeRecordModel(BinaryData.class)
    .withField("json", BINARY.asJson(), BinaryData::jsonData)
    .withField("enum", BINARY.asEnum(), BinaryData::enumData)
    .withField("string", BINARY.asString(), BinaryData::stringData);
```

## Working with Non-Record Classes

Write Models work with any Java class, not just records:

```java
public class LegacyData {
    private String id;
    private int value;
    private List<String> tags;

    // constructors, getters, setters...
    public String getId() { return id; }
    public int getValue() { return value; }
    public List<String> getTags() { return tags; }
}

var writeModel = writeRecordModel(LegacyData.class)
    .withField("identifier", STRING, LegacyData::getId)
    .withField("value", INTEGER, LegacyData::getValue)
    .withField("tags", LIST.ofType(STRING), LegacyData::getTags);
```

## Dynamic Model Creation

You can build Write Models dynamically at runtime based on configuration or metadata. This is particularly useful for:

- Creating flexible data export systems
- Adapting to changing schemas without code changes
- Creating generic data processing utilities


```java
// Version-aware model creation
public static WriteRecordModelType<Foo> createVersionedModel(int schemaVersion) {

    var modelBuilder = writeRecordModel(Foo.class);

    // Base fields available in all versions
    modelBuilder
        .withField("id", STRING, Foo::id)
        .withField("name", STRING, Foo::name);

    // Version-specific fields
    if (schemaVersion >= 2) {
        modelBuilder.withField("email", STRING, Foo::email);
    }

    if (schemaVersion >= 3) {
        modelBuilder.withField("created_at", INSTANT, Foo::createdAt);
    }
    return modelBuilder;
}
```


## Best Practices

### When to Use Write Models

**Use Write Models when:**

- Working with legacy classes that aren't records
- Need to rename fields in the Parquet output
- Want to write only specific fields from your objects
- Need to compute derived fields
- Require specific data type control
- Working with runtime-determined schemas

**Use automatic mapping when:**

- Java records map directly to desired Parquet schema
- Field names and types are already correct
- Simple, straightforward data serialization is needed

### Performance Considerations

- Write Models have no performance overhead compared to automatic mapping
- Accessor functions are called once per field per instance
- Complex computations in accessors can impact performance
- Consider caching expensive calculations

### Error Handling

Write Models doesn't validate that field function match with the specified field type. Following code will throw a ClassCastException at runtime trying to write the file:

```java
// This will throw an exception - mismatched class types
var invalidModel = writeRecordModel(Person.class)
    .withField("name", INTEGER, Person::name); // String -> INTEGER mismatch
```

Accessor functions should handle null values appropriately:

```java
var writeModel = writeRecordModel(Person.class)
    .withField("name_length", INTEGER, person ->
        person.name() != null ? person.name().length() : 0);
```

## Complete Example

Here's a comprehensive example showing various Write Model features:

```java
import static com.jerolba.carpet.model.FieldTypes.*;

enum OrderStatus { PENDING, CONFIRMED, SHIPPED, DELIVERED }

record Item(String sku, int quantity, BigDecimal price) {}

record CustomerOrder(
    UUID orderId,
    String customerEmail,
    List<Item> items,
    OrderStatus status,
    LocalDateTime createdAt,
    Map<String, String> metadata
) {}

// Create a comprehensive Write Model
var writeModel = writeRecordModel(CustomerOrder.class)
    // Basic field mapping with renaming
    .withField("order_id", UUID, CustomerOrder::orderId)
    .withField("customer_email", STRING, CustomerOrder::customerEmail)

    // Computed fields
    .withField("total_items", INTEGER, order ->
        order.items().stream().mapToInt(Item::quantity).sum())
    .withField("total_amount", BIG_DECIMAL.withPrecisionScale(10, 2), order ->
        order.items().stream()
            .map(item -> item.price().multiply(BigDecimal.valueOf(item.quantity())))
            .reduce(BigDecimal.ZERO, BigDecimal::add))

    // Complex collections
    .withField("item_skus", LIST.ofType(STRING), order ->
        order.items().stream().map(Item::sku).toList())

    // Enum handling
    .withField("status", ENUM.ofType(OrderStatus.class), CustomerOrder::status)

    // Date/time fields
    .withField("created_at", LOCAL_DATE_TIME, CustomerOrder::createdAt)
    .withField("created_date", LOCAL_DATE, order -> order.createdAt().toLocalDate())

    // Map fields
    .withField("metadata", MAP.ofTypes(STRING, STRING), CustomerOrder::metadata);

// Use the Write Model
try (CarpetWriter<CustomerOrder> writer = new CarpetWriter.Builder<>(outputStream, CustomerOrder.class)
        .withWriteRecordModel(writeModel)
        .build()) {
    writer.write(orders);
}
```

This example demonstrates field renaming, computed fields, collection handling, enum mapping, date/time processing, and complex data type configuration - all the key features of Carpet's Write Model system.
