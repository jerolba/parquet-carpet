# Input and Output Files

`parquet-java` defines `OutputFile` and `InputFile` interfaces to interact with files. Originally, it only provided `HadoopOutputFile` and `HadoopInputFile` implementations that were capable of working with Hadoop and local files.

This required a Hadoop dependency to be included in the project. This is not ideal for projects that only need to work with local files, as it adds unnecessary complexity and size to the project. To address this, Parquet Java recently added `LocalOutputFile` and `LocalInputFile` implementations.

Before these classes were created, Carpet provided a local file implementation with `FileSystemOutputFile` and `FileSystemInputFile`. You can use either implementation.
