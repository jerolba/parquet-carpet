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

import static java.lang.Character.isLowerCase;
import static java.lang.Character.isUpperCase;
import static java.lang.Character.toLowerCase;

public class CaseConverter {

    public static String camelCaseToSnakeCase(String camelCase) {
        if (camelCase == null || camelCase.isEmpty()) {
            return camelCase;
        }

        StringBuilder snakeCase = new StringBuilder();
        int i = 0;
        while (i < camelCase.length() && camelCase.charAt(i) == '_') {
            i++;
        }
        for (; i < camelCase.length(); i++) {
            char currentChar = camelCase.charAt(i);
            if (isUpperCase(currentChar)) {
                if (i > 0 && camelCase.charAt(i - 1) != '_') {
                    snakeCase.append('_');
                }
                snakeCase.append(toLowerCase(currentChar));
                // If following chars are Uppercase, lowercase it until end of find first
                // lowercase
                while (i + 1 < camelCase.length() && isUpperCase(camelCase.charAt(i + 1))) {
                    if (i + 2 < camelCase.length()) {
                        if (isLowerCase(camelCase.charAt(i + 2))) {
                            snakeCase.append('_');
                            snakeCase.append(toLowerCase(camelCase.charAt(i + 1)));
                            i++;
                            snakeCase.append(camelCase.charAt(i + 1));
                            i++;
                        } else {
                            snakeCase.append(toLowerCase(camelCase.charAt(i + 1)));
                            i++;
                        }
                    } else {
                        snakeCase.append(toLowerCase(camelCase.charAt(i + 1)));
                        i++;
                    }
                }
            } else {
                snakeCase.append(currentChar);
            }
        }
        if (snakeCase.length() == 0) {
            return camelCase;
        }
        return snakeCase.toString();
    }

}
