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
package com.jerolba.carpet.impl;

import java.lang.reflect.RecordComponent;

import com.jerolba.carpet.annotation.Alias;

public class AliasField {

    public static String getFieldName(RecordComponent recordComponent) {
        Alias annotation = recordComponent.getAnnotation(Alias.class);
        if (annotation == null) {
            return recordComponent.getName();
        }
        return annotation.value();
    }

    public static String getComponentAlias(RecordComponent recordComponent) {
        Alias annotation = recordComponent.getAnnotation(Alias.class);
        if (annotation != null) {
            return annotation.value();
        }
        return null;
    }

}
