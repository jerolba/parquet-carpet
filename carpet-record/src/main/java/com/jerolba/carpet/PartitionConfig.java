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

import java.util.List;

/**
 * Configuration for partitioned writing.
 * 
 * @param <T> The type of records being partitioned
 * @param keys List of partition keys
 * @param basePath Base directory path for partitions
 * @param autoCreateDirectories Whether to automatically create directories
 * @param maxPathLength Maximum allowed path length
 */
public record PartitionConfig<T>(
    List<PartitionKey<T>> keys,
    String basePath,
    boolean autoCreateDirectories,
    int maxPathLength
) {
    
    public PartitionConfig {
        if (basePath == null || basePath.trim().isEmpty()) {
            throw new IllegalArgumentException("Base path cannot be null or empty");
        }
        if (keys == null || keys.isEmpty()) {
            throw new IllegalArgumentException("At least one partition key must be defined");
        }
        if (maxPathLength <= 0) {
            throw new IllegalArgumentException("Max path length must be positive");
        }
    }
}
