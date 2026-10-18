# S3 Files

`carpet-s3` adds the ability to read and write Parquet files **directly from/to Amazon S3** (or other S3-compatible services) without requiring you to copy files to a local filesystem. It provides `S3InputFile` and `S3OutputFile` implementations that handle all the complexity of S3 access, including ranged reads, footer caching, and multipart uploads.

Designed as a standalone I/O module, `carpet-s3` can be used with Carpet or **any other Parquet Java library** based on `parquet-java`, and it does not require the core Carpet library as a dependency. It is built on top of the [AWS S3 SDK](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/home.html), which is resolved automatically when you include `carpet-s3` in your project.

## Getting started

Add the `carpet-s3` dependency to your project:

--8<-- "carpet-s3-dependency.md"

Depending on your S3 client security configuration, you may also need to add the [AWS Security Token Service dependency](https://central.sonatype.com/artifact/software.amazon.awssdk/sts/overview) to your project. This is required if you are using temporary credentials or assuming roles.

## Reading from S3

The simplest way to read a Parquet file from S3:

```java
InputFile inputFile = S3InputFile.of("s3://my-bucket/data.parquet");
List<MyRecord> records = new CarpetReader<>(inputFile, MyRecord.class).toList();
```

## Writing to S3

Writing a Parquet file to S3 works similarly:

```java
OutputFile outputFile = S3OutputFile.of("s3://my-bucket/data.parquet");
try (CarpetWriter<MyRecord> writer = new CarpetWriter<>(outputFile, MyRecord.class)) {
    writer.write(records);
}
```

## S3 Client configuration

Carpet S3 relies on the [AWS S3 SDK](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/home.html) to communicate with S3-compatible servers. By default, it uses the default client configuration for your environment (equivalent to calling `S3Client.create()`). Credentials, region, and other settings are resolved automatically from the default AWS credential and configuration chains.

If you need to customize the S3 client, you can build one with your own configuration and pass it to the file builders or provide it as system properties at runtime.

## Builder API

For granular control, use the builder to supply a custom `S3Client`. For example, to connect to a local S3-compatible server like [LocalStack](https://localstack.cloud/):

```java
S3Client client = S3Client.builder()
    .endpointOverride(localStack.getEndpoint())
    .credentialsProvider(() -> AwsBasicCredentials.create(localStack.getAccessKey(), localStack.getSecretKey()))
    .region(Region.of(localStack.getRegion()))
    .build();

S3InputFile inputFile = S3InputFile.builder("s3://bucket/data.parquet")
    .s3Client(client)
    .build();

S3OutputFile outputFile = S3OutputFile.builder("s3://bucket/data.parquet")
    .s3Client(client)
    .build();
```

## System properties or environment variables

You can set the AWS SDK system properties to configure the client globally and connect to AWS S3, Cloudflare R2, MinIO, or any other S3-compatible service:

```java
// Set the system properties via -D flags or programmatically
System.setProperty("aws.endpointUrl", r2Configuration.getEndpoint());
System.setProperty("aws.accessKeyId", r2Configuration.getAccessKey());
System.setProperty("aws.secretAccessKey", r2Configuration.getSecretKey());
System.setProperty("aws.region", "auto");

S3InputFile inputFile = S3InputFile.of("s3://bucket/data.parquet");

S3OutputFile outputFile = S3OutputFile.of("s3://bucket/data.parquet");
```

Or set the equivalent environment variables:

```bash
export AWS_ENDPOINT_URL=https://xxxxxxxx.r2.cloudflarestorage.com
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_REGION=auto
```

System properties and environment variables are resolved automatically by the AWS SDK, so you can use either approach depending on your deployment environment. System properties take precedence over environment variables.

## S3OutputFile configuration

The `S3OutputFile` builder allows you to configure fine-grained options for uploads, such as the S3 client, concurrency level, and custom executor for parallel uploads. The builder provides a fluent API for setting these options.

### URL configuration

You can configure the S3 URL directly in the builder method, which allows you to specify the bucket and object key. The URL must start with `s3://` and include the bucket name and object key.

```java
S3OutputFile outputFile = S3OutputFile
    .builder("s3://bucket/path/to/data.parquet")
    .build();
```

Alternatively, you can configure the bucket and key separately:

```java
S3OutputFile outputFile = S3OutputFile
    .builder()
    .bucket("bucket")
    .key("path/to/data.parquet")
    .build();
```

### Upload configuration

Carpet S3 uses multipart uploads to handle large files efficiently. To minimize latency, each chunk of the configured size is uploaded in the background as soon as Parquet writes it.

You can configure the part size and concurrency for uploads using the builder:

```java
S3OutputFile outputFile = S3OutputFile.builder()
    .s3Path("s3://bucket/path/to/data.parquet")
    .partSize(30 * 1024 * 1024) // 30 MiB
    .concurrency(4)
    .build();
```

- **Part Size**: The default part size is 16 MiB (one-eighth of the configured row group size). The minimum part size is 5 MiB (the S3 API minimum), with a maximum of 5 GiB.

- **Concurrency**: To prevent saturating the network, the number of concurrent uploads is limited. The default concurrency level is 4.

During the upload process, Virtual Threads are used if available in the JVM; otherwise, a `new FixedThreadPool(concurrency)` executor is used. You can also override this behavior by providing a custom `Executor`:

```java
S3OutputFile outputFile = S3OutputFile.builder("s3://bucket/data.parquet")
    .executor(myExecutor)
    .build();
```

!!! note
    `concurrency` and `executor` are mutually exclusive — use one or the other.

If concurrency is set to 1, the file is uploaded sequentially without any parallelism.

## S3InputFile configuration

Similarly, the `S3InputFile` builder provides a fluent API for optimizing read performance.

### URL configuration

Define the target file by providing the full URL or explicitly setting the bucket and key:

```java
// Using a full S3 URI
S3InputFile inputFile = S3InputFile
    .builder("s3://bucket/path/to/data.parquet")
    .build();

// Using separate components
S3InputFile inputFile = S3InputFile
    .builder()
    .bucket("bucket")
    .key("path/to/data.parquet")
    .build();
```

### Download configuration

Downloading from S3 is performed in parallel using ranged GET requests. At the same time, speculative prefetching improves performance: based on the columns requested for the previous row group, the system predicts and downloads relevant data in advance.

You can define the concurrency level:

```java
S3InputFile inputFile = S3InputFile.builder("s3://bucket/path/to/data.parquet")
    .concurrency(4)
    .build();
```
When concurrency is configured to 1, the file is downloaded sequentially without any parallelism.

If no executor is provided, Virtual Threads are used if available in the JVM. Otherwise, a default executor is created with a thread pool matching the configured concurrency (default is 4). You can also supply a custom `Executor`:

```java
S3InputFile inputFile = S3InputFile.builder("s3://bucket/path/to/data.parquet")
    .executor(myExecutor)
    .build();
```

!!! note
    `concurrency` and `executor` are mutually exclusive — use one or the other.

Speculative downloading (prefetching) is enabled by default. If it is unnecessary for your use case (e.g., heavy row group pruning renders prefetching ineffective or increases memory usage), you can disable it via the builder or the `carpet.s3.prefetch.enabled` system property:

```java
S3InputFile inputFile = S3InputFile.builder("s3://bucket/path/to/data.parquet")
    .prefetch(false)
    .build();
```

## Local files fallback

When configured with a local file path instead of an `s3://` URL, both `S3InputFile` and `S3OutputFile` automatically adapt. This allows you to use the exact same code across local development, test suites, and production S3 environments:

```java
// Works with local files without any code changes
InputFile inputFile = S3InputFile.of("/tmp/local-file.parquet");
OutputFile outputFile = S3OutputFile.of("/tmp/output.parquet");
```

If a path is not an S3 URL and the file (or its parent directory) exists locally, the builder transparently creates a local file implementation.

## Additional configurations

### Footer caching

Parquet stores essential metadata (the footer) at the end of the file. To avoid repeated S3 requests when parsing the schema and row group details, `carpet-s3` eagerly fetches the last 8 MiB of the file. Files smaller than 8 MiB are cached entirely in a single request.

You can adjust the cache size via the `carpet.footer.cache.size` system property (in bytes). The default is 8 MiB:

```bash
java -Dcarpet.footer.cache.size=10485760 -jar yourapp.jar
```

### Pre-download mode

For development scenarios involving repeated reads of the same large file across multiple executions, you can enable pre-downloading to a local temporary file to save bandwidth and execution time:

```bash
java -Dcarpet.s3.predownload.file=true -jar yourapp.jar
```

When enabled, the file is downloaded to the system's temp directory (configurable via `java.io.tmpdir`). The downloaded file is cached using the S3 object's ETag, ensuring subsequent reads instantly reuse the local copy unless the remote file changes. If the ETag changes upstream, the file is safely downloaded again. Temporary files **are not deleted automatically**, allowing you to inspect or reuse them across different JVM executions.