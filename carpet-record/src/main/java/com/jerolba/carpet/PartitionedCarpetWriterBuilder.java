/**
 * Copyright 2023 Jerónimo López Bezanilla
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jerolba.carpet;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Builder for creating PartitionedCarpetWriter instances.
 *
 * @param <T> The type of records to write
 */
public class PartitionedCarpetWriterBuilder<T> {
    private final Class<T> recordClass;
    private final List<PartitionKey<T>> keys;
    private String basePath;
    private boolean autoCreateDirectories;
    private int maxPathLength;
    
    /**
     * Constructs a new builder for the specified record class.
     *
     * @param recordClass The class of records to write
     * @throws IllegalArgumentException if recordClass is null
     */
    public PartitionedCarpetWriterBuilder(Class<T> recordClass) {
        if (recordClass == null) {
            throw new IllegalArgumentException("Record class cannot be null");
        }
        this.recordClass = recordClass;
        this.keys = new ArrayList<>();
        this.autoCreateDirectories = true;
        this.maxPathLength = 4096; // Default reasonable limit
    }
    
    /**
     * Sets the base path for partitions.
     *
     * @param basePath The base directory path
     * @return This builder
     * @throws IllegalArgumentException if basePath is null or empty
     */
    public PartitionedCarpetWriterBuilder<T> withBasePath(String basePath) {
        if (basePath == null || basePath.trim().isEmpty()) {
            throw new IllegalArgumentException("Base path cannot be null or empty");
        }
        this.basePath = basePath;
        return this;
    }
    
    /**
     * Adds a partition key with the specified name and extraction function.
     *
     * @param name The name of the partition key
     * @param extractor Function to extract the partition value from a record
     * @return This builder
     * @throws IllegalArgumentException if name is null/empty or extractor is null
     */
    public PartitionedCarpetWriterBuilder<T> partitionBy(String name, Function<T, String> extractor) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Partition name cannot be null or empty");
        }
        if (extractor == null) {
            throw new IllegalArgumentException("Partition extractor cannot be null");
        }
        
        // Check for duplicate partition names
        for (PartitionKey<T> existingKey : keys) {
            if (existingKey.name().equals(name)) {
                throw new IllegalArgumentException("Duplicate partition key name: " + name);
            }
        }
        
        keys.add(new PartitionKey<>(name, extractor));
        return this;
    }
    
    /**
     * Sets the maximum allowed path length for partitions.
     *
     * @param maxPathLength The maximum path length in characters
     * @return This builder
     * @throws IllegalArgumentException if maxPathLength is not positive
     */
    public PartitionedCarpetWriterBuilder<T> withMaxPathLength(int maxPathLength) {
        if (maxPathLength <= 0) {
            throw new IllegalArgumentException("Max path length must be positive");
        }
        this.maxPathLength = maxPathLength;
        return this;
    }
    
    /**
     * Sets whether to automatically create directories.
     *
     * @param autoCreateDirectories Whether to auto-create directories
     * @return This builder
     */
    public PartitionedCarpetWriterBuilder<T> withAutoCreateDirectories(boolean autoCreateDirectories) {
        this.autoCreateDirectories = autoCreateDirectories;
        return this;
    }
    
    /**
     * Builds a PartitionedCarpetWriter with the current configuration.
     *
     * @return A new PartitionedCarpetWriter instance
     * @throws IllegalStateException if required configuration is missing
     * @throws IOException if the writer cannot be created
     */
    public PartitionedCarpetWriter<T> build() throws IOException {
        // Final validation before building
        if (basePath == null) {
            throw new IllegalStateException("Base path must be set using withBasePath()");
        }
        if (keys.isEmpty()) {
            throw new IllegalStateException("At least one partition key must be defined using partitionBy()");
        }
        
        // Validate base path exists and is writable
        File basePathFile = new File(basePath);
        if (basePathFile.exists() && !basePathFile.canWrite()) {
            throw new IOException("Base path is not writable: " + basePath);
        }
        
        PartitionConfig<T> config = new PartitionConfig<>(keys, basePath, autoCreateDirectories, maxPathLength);
        return new PartitionedCarpetWriter<>(config, recordClass);
    }
}
