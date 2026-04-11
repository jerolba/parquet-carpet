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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.parquet.io.SeekableInputStream;

class LocalInputFile implements S3InputFile {

    private final Path localPath;

    public LocalInputFile(Path localPath) {
        this.localPath = localPath;
    }

    @Override
    public long getLength() throws IOException {
        return Files.size(localPath);
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        return new LocalFileInputStream(localPath);
    }

}
