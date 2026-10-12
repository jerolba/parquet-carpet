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

import static com.jerolba.carpet.io.s3.SeekableReaderHelper.readerOf;
import static com.jerolba.carpet.io.s3.SeekableReaderHelper.sequentialBytes;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import com.jerolba.carpet.io.s3.PrefetchTestSupport.ManualExecutor;

class RangeReaderFactoryTest {

    private final SeekableReader reader = readerOf(sequentialBytes(100));
    private final PrefetchConfig prefetchOn = new PrefetchConfig(true, 1024);

    @Test
    void withoutExecutorReadsSequentiallyEvenIfPrefetchIsEnabled() {
        try (RangeReader created = RangeReaderFactory.create(reader, null, prefetchOn)) {
            assertInstanceOf(SequentialReader.class, created);
        }
    }

    @Test
    void withExecutorAndPrefetchDisabledReadsVectored() {
        try (RangeReader created = RangeReaderFactory.create(reader, ManualExecutor::new,
                PrefetchConfig.disabled())) {
            assertInstanceOf(VectorReader.class, created);
        }
    }

    @Test
    void fileFullyCachedInMemoryIsNotPrefetched() throws IOException {
        var cached = new CachedFooterProxyReader(reader, 1024);
        try (RangeReader created = RangeReaderFactory.create(cached, ManualExecutor::new, prefetchOn)) {
            assertInstanceOf(VectorReader.class, created);
        }
    }

    @Test
    void fileLargerThanTheCacheIsPrefetched() throws IOException {
        var cached = new CachedFooterProxyReader(reader, 50);
        try (RangeReader created = RangeReaderFactory.create(cached, ManualExecutor::new, prefetchOn)) {
            assertInstanceOf(PrefetchingRangeReader.class, created);
        }
    }

    @Test
    void withExecutorAndPrefetchEnabledDecoratesTheVectoredReader() {
        try (RangeReader created = RangeReaderFactory.create(reader, ManualExecutor::new, prefetchOn)) {
            assertInstanceOf(PrefetchingRangeReader.class, created);
        }
    }

}
