/**
 * Copyright 2026 Jerónimo López Bezanilla
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
package com.jerolba.carpet.io.s3;

import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolved configuration of the speculative row group prefetch.
 *
 */
class PrefetchConfig {

    private static final Logger logger = LoggerFactory.getLogger(PrefetchConfig.class);

    /** Default cap of speculative bytes per stream: 256 MiB. */
    public static final int DEFAULT_MAX_BYTES = 256 * 1024 * 1024;

    private final boolean enabled;
    private final int maxBytes;

    /**
     * @param enabled  whether the next row group is prefetched while the current
     *                 one is being decoded
     * @param maxBytes maximum number of bytes a single speculation may hold
     */
    PrefetchConfig(Boolean enabled, Integer maxBytes) {
        this.enabled = enabled != null ? enabled
                : booleanProperty(S3InputFile.CARPET_S3_PREFETCH_ENABLED, true);
        this.maxBytes = maxBytes != null ? maxBytes
                : intProperty(S3InputFile.CARPET_S3_PREFETCH_MAX_BYTES, DEFAULT_MAX_BYTES);
        if (this.maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be > 0, got: " + this.maxBytes);
        }
    }

    private static boolean booleanProperty(String name, boolean defaultValue) {
        String value = System.getProperty(name);
        if (value == null) {
            return defaultValue;
        }
        if ("true".equalsIgnoreCase(value.trim())) {
            return true;
        }
        if ("false".equalsIgnoreCase(value.trim())) {
            return false;
        }
        logger.warn("Ignoring invalid boolean value '{}' of system property {}, using {}", value, name, defaultValue);
        return defaultValue;
    }

    private static int intProperty(String name, int defaultValue) {
        String value = System.getProperty(name);
        if (value == null) {
            return defaultValue;
        }
        try {
            int parsed = Integer.parseInt(value.trim());
            if (parsed > 0) {
                return parsed;
            }
        } catch (NumberFormatException e) {
            // fall through
        }
        logger.warn("Ignoring invalid value '{}' of system property {}, using {}", value, name, defaultValue);
        return defaultValue;
    }

    /**
     * @param seekableReader reader every physical read goes through, normally a
     *                       {@link CachedFooterProxyReader} so the footer is served
     *                       from memory; the prefetch decodes the footer through it
     *                       as well
     * @param executor       executor for vectored reads, or null for sequential
     *                       reads in the calling thread
     * @return the range reader for that configuration
     */
    RangeReader create(SeekableReader seekableReader, Executor executor) {
        if (executor == null) {
            return new SequentialReader(seekableReader);
        }
        if (!enabled) {
            return new VectorReader(seekableReader, executor);
        }
        logger.debug("Speculative prefetch enabled, max {} bytes", maxBytes);
        return new PrefetchingRangeReader(executor, seekableReader, maxBytes);
    }

    public boolean enabled() {
        return enabled;
    }

    public int maxBytes() {
        return maxBytes;
    }

}
