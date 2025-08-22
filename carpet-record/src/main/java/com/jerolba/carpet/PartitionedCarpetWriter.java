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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import com.jerolba.carpet.io.FileSystemOutputFile;

/**
 * A partitioned Parquet file writer that organizes data into a hierarchical directory structure
 * based on partition keys. This class wraps the existing CarpetWriter functionality.
 *
 * @param <T> The type of records to write
 */
public class PartitionedCarpetWriter<T> implements Closeable {
    
    private final PartitionConfig<T> config;
    private final Map<String, CarpetWriter<T>> partitionWriters;
    private final Class<T> recordClass;
    private final Set<String> createdPartitions;
    private final List<String> failedPartitions;
    
    /**
     * Constructs a PartitionedCarpetWriter with the specified configuration.
     *
     * @param config The partition configuration
     * @param recordClass The class of the records to write
     * @throws IllegalArgumentException if configuration is invalid
     */
    public PartitionedCarpetWriter(PartitionConfig<T> config, Class<T> recordClass) {
        validateConfiguration(config, recordClass);
        this.config = config;
        this.recordClass = recordClass;
        this.partitionWriters = new ConcurrentHashMap<>();
        this.createdPartitions = new HashSet<>();
        this.failedPartitions = new ArrayList<>();
    }
    
    private void validateConfiguration(PartitionConfig<T> config, Class<T> recordClass) {
        if (config == null) {
            throw new IllegalArgumentException("Partition configuration cannot be null");
        }
        if (recordClass == null) {
            throw new IllegalArgumentException("Record class cannot be null");
        }
        
        // Validate base path
        File basePathFile = new File(config.basePath());
        if (basePathFile.exists() && !basePathFile.isDirectory()) {
            throw new IllegalArgumentException("Base path exists but is not a directory: " + config.basePath());
        }
        
        // Validate partition keys
        Set<String> keyNames = new HashSet<>();
        for (PartitionKey<T> key : config.keys()) {
            if (keyNames.contains(key.name())) {
                throw new IllegalArgumentException("Duplicate partition key name: " + key.name());
            }
            keyNames.add(key.name());
        }
    }
    
    /**
     * Writes a single record to the appropriate partition.
     *
     * @param record The record to write
     * @throws IOException if an I/O error occurs
     * @throws NullPartitionValueException if a partition value is null
     * @throws PathTooLongException if the partition path exceeds the maximum length
     */
    public void write(T record) throws IOException {
        String partitionPath = null;
        try {
            partitionPath = calculatePartitionPath(record);
            CarpetWriter<T> writer = getOrCreateWriter(partitionPath);
            writer.write(record);
            createdPartitions.add(partitionPath);
        } catch (Exception e) {
            // Track failed partitions for cleanup
            if (partitionPath != null) {
                failedPartitions.add(partitionPath);
            } else {
                // If partition path calculation failed, try to construct a basic path for tracking
                try {
                    String basicPath = constructBasicPartitionPath(record);
                    failedPartitions.add(basicPath);
                } catch (Exception pathException) {
                    // If even basic path construction fails, add a generic entry
                    failedPartitions.add(config.basePath() + "/failed_partition");
                }
            }
            throw e;
        }
    }
    
    private String constructBasicPartitionPath(T record) {
        StringBuilder path = new StringBuilder(config.basePath());
        for (PartitionKey<T> key : config.keys()) {
            try {
                String partitionValue = key.extractor().apply(record);
                if (partitionValue == null) {
                    partitionValue = "null";
                }
                path.append("/").append(key.name()).append("=").append(partitionValue);
            } catch (Exception e) {
                path.append("/").append(key.name()).append("=error");
            }
        }
        return path.toString();
    }
    
    /**
     * Writes a collection of records to their appropriate partitions.
     *
     * @param records The collection of records to write
     * @throws IOException if an I/O error occurs
     */
    public void write(Collection<T> records) throws IOException {
        for (T record : records) {
            write(record);
        }
    }
    
    /**
     * Writes a stream of records to their appropriate partitions.
     *
     * @param stream The stream of records to write
     * @throws IOException if an I/O error occurs
     */
    public void write(Stream<T> stream) throws IOException {
        stream.forEach(record -> {
            try {
                write(record);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }
    
    private String calculatePartitionPath(T record) {
        StringBuilder path = new StringBuilder(config.basePath());
        
        for (PartitionKey<T> key : config.keys()) {
            String partitionValue = key.extractor().apply(record);
            
            // Fail fast on null partition values
            if (partitionValue == null) {
                throw new NullPartitionValueException(
                    "Null partition value not allowed for key: " + key.name() + 
                    " in record: " + record);
            }
            
            // Validate path length before adding
            String newPath = path + "/" + key.name() + "=" + partitionValue;
            if (newPath.length() > config.maxPathLength()) {
                throw new PathTooLongException(
                    "Partition path exceeds maximum length (" + config.maxPathLength() + 
                    "): " + newPath.length() + " characters");
            }
            
            path.append("/").append(key.name()).append("=").append(partitionValue);
        }
        
        return path.toString();
    }
    
    private CarpetWriter<T> getOrCreateWriter(String partitionPath) throws IOException {
        return partitionWriters.computeIfAbsent(partitionPath, path -> {
            try {
                createPartitionDirectory(path);
                return new CarpetWriter<>(new FileSystemOutputFile(new File(path + "/data.parquet")), recordClass);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }
    
    private void createPartitionDirectory(String partitionPath) throws IOException {
        File directory = new File(partitionPath);
        if (!directory.exists()) {
            if (!directory.mkdirs()) {
                throw new IOException("Failed to create partition directory: " + partitionPath);
            }
        }
    }
    
    /**
     * Cleans up empty partition directories that were created but never written to.
     *
     * @throws IOException if cleanup fails
     */
    public void cleanupEmptyPartitions() throws IOException {
        for (String partitionPath : createdPartitions) {
            File partitionDir = new File(partitionPath);
            File dataFile = new File(partitionPath + "/data.parquet");
            
            if (partitionDir.exists() && (!dataFile.exists() || dataFile.length() == 0)) {
                if (dataFile.exists()) {
                    dataFile.delete();
                }
                if (partitionDir.delete()) {
                    createdPartitions.remove(partitionPath);
                }
            }
        }
    }
    
    /**
     * Validates that all created partitions have valid data files.
     * Note: This method should be called after closing the writer to ensure data is flushed.
     *
     * @throws PartitionIntegrityException if any partition is invalid
     */
    public void validatePartitionIntegrity() throws IOException {
        List<String> invalidPartitions = new ArrayList<>();
        
        for (String partitionPath : createdPartitions) {
            File dataFile = new File(partitionPath + "/data.parquet");
            if (!dataFile.exists() || dataFile.length() == 0) {
                invalidPartitions.add(partitionPath);
            }
        }
        
        if (!invalidPartitions.isEmpty()) {
            throw new PartitionIntegrityException(
                "Found invalid partitions: " + String.join(", ", invalidPartitions));
        }
    }
    
    /**
     * Validates that all created partitions have valid data files.
     * This method flushes all writers before validation.
     *
     * @throws PartitionIntegrityException if any partition is invalid
     */
    public void validatePartitionIntegrityWithFlush() throws IOException {
        // Close all writers to flush data
        for (CarpetWriter<T> writer : partitionWriters.values()) {
            writer.close();
        }
        partitionWriters.clear();
        
        validatePartitionIntegrity();
    }
    
    /**
     * Gets list of partitions that failed during writing.
     *
     * @return List of failed partition paths
     */
    public List<String> getFailedPartitions() {
        return new ArrayList<>(failedPartitions);
    }
    
    /**
     * Gets set of successfully created partitions.
     *
     * @return Set of created partition paths
     */
    public Set<String> getCreatedPartitions() {
        return new HashSet<>(createdPartitions);
    }
    
    /**
     * Attempts to recover from failed partitions by cleaning up and retrying.
     *
     * @throws IOException if recovery fails
     */
    public void recoverFromFailures() throws IOException {
        // Clean up any partial data from failed partitions
        for (String failedPartition : failedPartitions) {
            File partitionDir = new File(failedPartition);
            if (partitionDir.exists()) {
                File dataFile = new File(failedPartition + "/data.parquet");
                if (dataFile.exists()) {
                    dataFile.delete();
                }
                if (partitionDir.list() == null || partitionDir.list().length == 0) {
                    partitionDir.delete();
                }
            }
        }
        failedPartitions.clear();
    }
    
    @Override
    public void close() throws IOException {
        try {
            for (CarpetWriter<T> writer : partitionWriters.values()) {
                writer.close();
            }
            partitionWriters.clear();
        } finally {
            // Always attempt cleanup on close
            try {
                cleanupEmptyPartitions();
            } catch (Exception e) {
                // Log but don't throw during close
                System.err.println("Warning: Failed to cleanup empty partitions: " + e.getMessage());
            }
        }
    }
    
    /**
     * Closes the writer without cleaning up empty partitions.
     * Useful for testing and validation scenarios.
     */
    public void closeWithoutCleanup() throws IOException {
        for (CarpetWriter<T> writer : partitionWriters.values()) {
            writer.close();
        }
        partitionWriters.clear();
    }
}
