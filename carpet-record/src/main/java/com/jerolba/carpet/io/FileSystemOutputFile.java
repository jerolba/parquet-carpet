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
package com.jerolba.carpet.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

/**
 *
 * Class for writing to a file on the file system using the Parquet output file
 * interface.
 *
 * @author jerolba
 */
public class FileSystemOutputFile implements OutputFile {

    private final File file;

    /**
     *
     * Constructs a FileSystemOutputFile with the specified file.
     *
     * @param file the file to write to
     */
    public FileSystemOutputFile(File file) {
        this.file = file;
    }

    /**
     *
     * Creates an output stream for writing to the file.
     *
     * @param blockSizeHint the block size hint, ignored by this implementation
     * @return a new PositionOutputStream for writing to the file
     * @throws IOException if the file already exists or an error occurs while
     *                     creating the output stream
     */
    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
        if (file.exists()) {
            throw new IllegalArgumentException("File already exists: " + file);
        }
        return createOrOverwrite(blockSizeHint);
    }

    /**
     *
     * Creates or overwrites an output stream for writing to the file.
     *
     * @param blockSizeHint the block size hint, ignored by this implementation
     * @return a new PositionOutputStream for writing to the file
     * @throws IOException if an error occurs while creating the output stream
     */
    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
        return new CountedPositionOutputStream(new FileOutputStream(file));
    }

    /**
     *
     * Returns false to indicate that this implementation does not support block
     * sizes.
     *
     * @return false
     */
    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    /**
     *
     * Returns 0 to indicate that this implementation does not support block sizes.
     *
     * @return 0
     */
    @Override
    public long defaultBlockSize() {
        return 0;
    }

}
