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

import java.lang.annotation.Annotation;
import java.lang.reflect.RecordComponent;

public class NotNullField {

    public static boolean isNotNull(RecordComponent recordComponent) {
        if (recordComponent.getType().isPrimitive()) {
            return true;
        }
        return isNotNullAnnotated(recordComponent.getDeclaredAnnotations());
    }

    public static boolean isNotNullAnnotated(Annotation[] annotations) {
        for (Annotation annotation : annotations) {
            var type = annotation.annotationType();
            String name = type.getSimpleName().toLowerCase();
            if (name.equals("nonnull") || name.equals("notnull")) {
                return true;
            }
        }
        return false;
    }

}
