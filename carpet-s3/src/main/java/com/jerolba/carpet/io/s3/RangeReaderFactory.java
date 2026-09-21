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
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Chooses the {@link RangeReader} behind a stream from its read configuration:
 * sequential reads in the calling thread when there is no executor, vectored
 * reads on the executor otherwise, decorated with the speculative prefetch of
 * the next row group when it is enabled.
 */
final class RangeReaderFactory {

    private static final Logger logger = LoggerFactory.getLogger(RangeReaderFactory.class);

    private RangeReaderFactory() {
    }

    /**
     * @param cachedReader reader every physical read goes through, normally a
     *                     {@link CachedFooterProxyReader} so the footer is served
     *                     from memory; the prefetch decodes the footer through it
     *                     as well
     * @param executorFactory supplies the executor for vectored reads, called once
     *                        per created reader, or null for sequential reads in the
     *                        calling thread
     * @param prefetchConfig  speculative prefetch configuration, only honoured when
     *                        an executor is available
     * @return the range reader for that configuration
     */
    static RangeReader create(SeekableReader cachedReader, Supplier<Executor> executorFactory,
            PrefetchConfig prefetchConfig) {
        if (executorFactory == null) {
            return new SequentialReader(cachedReader);
        }
        Executor executor = executorFactory.get();
        VectorReader vectorReader = new VectorReader(cachedReader, executor);
        if (!prefetchConfig.enabled()) {
            return vectorReader;
        }
        if (cachedReader instanceof CachedFooterProxyReader cached && cached.isCached(0)) {
            logger.debug("Whole file cached in memory, speculative prefetch not needed");
            return vectorReader;
        }
        logger.debug("Speculative prefetch enabled, max {} bytes", prefetchConfig.maxBytes());
        ParallelReader parallelReader = new ParallelReader(cachedReader, executor);
        return new PrefetchingRangeReader(vectorReader, parallelReader, cachedReader, prefetchConfig.maxBytes());
    }

}
