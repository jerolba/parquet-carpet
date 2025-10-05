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
package com.jerolba.carpet.impl.write;

import java.util.Map;

import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.api.WriteSupport;

import com.jerolba.carpet.WriteModelFactory;
import com.jerolba.carpet.WriteModelFactory.WriteConfigurationContext;
import com.jerolba.carpet.model.WriteRecordModelType;

public class WriteSupportFactory {

    private WriteSupportFactory() {
    }

    public static <T> WriteSupport<T> createWriteSupport(Class<T> recordClass, Map<String, String> extraMetaData,
            ParquetConfiguration parquetConfiguration, CarpetWriteConfiguration carpetConfiguration,
            WriteModelFactory<T> writeModelFactory) {

        if (writeModelFactory == null) {
            if (useWriteModel(parquetConfiguration)) {
                JavaRecord2WriteModel javaRecord2WriteModel = new JavaRecord2WriteModel(carpetConfiguration);
                WriteRecordModelType<T> rootWriteRecordModel = javaRecord2WriteModel.createModel(recordClass);
                return new WriteRecordModelWriteSupport<>(rootWriteRecordModel, extraMetaData, carpetConfiguration);
            }
            return new CarpetWriteSupport<>(recordClass, extraMetaData, carpetConfiguration);
        }
        var writeConfigurationContext = new WriteConfigurationContext(carpetConfiguration, parquetConfiguration);
        WriteRecordModelType<T> rootWriteRecordModel = writeModelFactory.create(recordClass, writeConfigurationContext);
        return new WriteRecordModelWriteSupport<>(rootWriteRecordModel, extraMetaData, carpetConfiguration);
    }

    private static boolean useWriteModel(ParquetConfiguration parquetConfiguration) {
        return parquetConfiguration.getBoolean("parquet.carpet.useJavaRecord2WriteModel", false)
                || System.getProperty("parquet.carpet.useJavaRecord2WriteModel") != null;
    }

}
