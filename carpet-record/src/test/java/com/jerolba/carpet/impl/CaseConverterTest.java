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

import static com.jerolba.carpet.impl.CaseConverter.camelCaseToSnakeCase;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class CaseConverterTest {

    @Test
    void camelToSnakeCaseTest() {
        assertEquals("html_parser", camelCaseToSnakeCase("HTMLParser"));
        assertEquals("foo", camelCaseToSnakeCase("FOO"));
        assertEquals("a", camelCaseToSnakeCase("a"));
        assertEquals("a", camelCaseToSnakeCase("A"));
        assertEquals("abc", camelCaseToSnakeCase("abc"));
        assertEquals("a_b", camelCaseToSnakeCase("aB"));
        assertEquals("u_id", camelCaseToSnakeCase("uId"));
        assertEquals("some_name", camelCaseToSnakeCase("someName"));
        assertEquals("some_name", camelCaseToSnakeCase("SomeName"));
        assertEquals("parser_html", camelCaseToSnakeCase("ParserHTML"));
        assertEquals("parser_js", camelCaseToSnakeCase("ParserJS"));
        assertEquals("parser_j_script", camelCaseToSnakeCase("ParserJScript"));
        assertEquals("x_men", camelCaseToSnakeCase("xMen"));
        assertEquals("www", camelCaseToSnakeCase("WWW"));
    }

    @Test
    void withNumbersTest() {
        // invalid java fields
        assertEquals("1", camelCaseToSnakeCase("1"));
        assertEquals("123", camelCaseToSnakeCase("123"));
        assertEquals("1a", camelCaseToSnakeCase("1a"));
        assertEquals("1_a", camelCaseToSnakeCase("1A"));

        // Valid java field
        assertEquals("a1", camelCaseToSnakeCase("a1"));
        assertEquals("a1", camelCaseToSnakeCase("A1"));
    }

    @Test
    void withSomeKindOfSnakeCaseTest() {
        assertEquals("some_name", camelCaseToSnakeCase("some_name"));
        assertEquals("some_name", camelCaseToSnakeCase("some_Name"));
        assertEquals("some_name", camelCaseToSnakeCase("Some_Name"));
        assertEquals("some__name", camelCaseToSnakeCase("some__Name"));
        assertEquals("some___name", camelCaseToSnakeCase("some___Name"));
        assertEquals("some_name", camelCaseToSnakeCase("_some_Name"));
        assertEquals("some_name", camelCaseToSnakeCase("_Some_Name"));
        assertEquals("some_name", camelCaseToSnakeCase("_SomeName"));
        assertEquals("some_name", camelCaseToSnakeCase("__some_Name"));
        assertEquals("some_name", camelCaseToSnakeCase("SOME_NAME"));
        assertEquals("x", camelCaseToSnakeCase("_x"));
        assertEquals("x", camelCaseToSnakeCase("_X"));
        assertEquals("x_x", camelCaseToSnakeCase("x_x"));
        assertEquals("x_x", camelCaseToSnakeCase("x_X"));
        assertEquals("x_x", camelCaseToSnakeCase("X_x"));
        assertEquals("x_x", camelCaseToSnakeCase("X_X"));
    }

    @Test
    void weirdCasesTest() {
        assertEquals(null, camelCaseToSnakeCase(null));
        assertEquals("", camelCaseToSnakeCase(""));
        assertEquals("_", camelCaseToSnakeCase("_"));
    }

    @Test
    void dolar() {
        assertEquals("$", camelCaseToSnakeCase("$"));
        assertEquals("$a", camelCaseToSnakeCase("$a"));
        assertEquals("a$", camelCaseToSnakeCase("a$"));
        assertEquals("$_a", camelCaseToSnakeCase("$_a"));
        assertEquals("a_$", camelCaseToSnakeCase("a_$"));
        assertEquals("a$a", camelCaseToSnakeCase("a$a"));
        assertEquals("$_a", camelCaseToSnakeCase("$A"));
        assertEquals("$_a", camelCaseToSnakeCase("$_A"));
    }

}
