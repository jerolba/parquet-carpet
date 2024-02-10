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

import static com.jerolba.carpet.impl.CaseConverter.camelToSnakeCase;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class CaseConverterTest {

    @Test
    public void camelToSnakeCaseTest() {
        assertEquals("html_parser", camelToSnakeCase("HTMLParser"));
        assertEquals("foo", camelToSnakeCase("FOO"));
        assertEquals("a", camelToSnakeCase("a"));
        assertEquals("a", camelToSnakeCase("A"));
        assertEquals("abc", camelToSnakeCase("abc"));
        assertEquals("a_b", camelToSnakeCase("aB"));
        assertEquals("u_id", camelToSnakeCase("uId"));
        assertEquals("some_name", camelToSnakeCase("someName"));
        assertEquals("some_name", camelToSnakeCase("SomeName"));
        assertEquals("parser_html", camelToSnakeCase("ParserHTML"));
        assertEquals("parser_js", camelToSnakeCase("ParserJS"));
        assertEquals("parser_j_script", camelToSnakeCase("ParserJScript"));
        assertEquals("x_men", camelToSnakeCase("xMen"));
        assertEquals("www", camelToSnakeCase("WWW"));
    }

    @Test
    public void withNumbersTest() {
        // invalid java fields
        assertEquals("1", camelToSnakeCase("1"));
        assertEquals("123", camelToSnakeCase("123"));
        assertEquals("1a", camelToSnakeCase("1a"));
        assertEquals("1_a", camelToSnakeCase("1A"));

        // Valid java field
        assertEquals("a1", camelToSnakeCase("a1"));
        assertEquals("a1", camelToSnakeCase("A1"));
    }

    @Test
    public void withSomeKindOfSnakeCaseTest() {
        assertEquals("some_name", camelToSnakeCase("some_name"));
        assertEquals("some_name", camelToSnakeCase("some_Name"));
        assertEquals("some_name", camelToSnakeCase("Some_Name"));
        assertEquals("some__name", camelToSnakeCase("some__Name"));
        assertEquals("some___name", camelToSnakeCase("some___Name"));
        assertEquals("some_name", camelToSnakeCase("_some_Name"));
        assertEquals("some_name", camelToSnakeCase("_Some_Name"));
        assertEquals("some_name", camelToSnakeCase("_SomeName"));
        assertEquals("some_name", camelToSnakeCase("__some_Name"));
        assertEquals("some_name", camelToSnakeCase("SOME_NAME"));
        assertEquals("x", camelToSnakeCase("_x"));
        assertEquals("x", camelToSnakeCase("_X"));
        assertEquals("x_x", camelToSnakeCase("x_x"));
        assertEquals("x_x", camelToSnakeCase("x_X"));
        assertEquals("x_x", camelToSnakeCase("X_x"));
        assertEquals("x_x", camelToSnakeCase("X_X"));
    }

    @Test
    public void weirdCasesTest() {
        assertEquals(null, camelToSnakeCase(null));
        assertEquals("", camelToSnakeCase(""));
        assertEquals("_", camelToSnakeCase("_"));
    }

    @Test
    public void dolar() {
        assertEquals("$", camelToSnakeCase("$"));
        assertEquals("$a", camelToSnakeCase("$a"));
        assertEquals("a$", camelToSnakeCase("a$"));
        assertEquals("$_a", camelToSnakeCase("$_a"));
        assertEquals("a_$", camelToSnakeCase("a_$"));
        assertEquals("a$a", camelToSnakeCase("a$a"));
        assertEquals("$_a", camelToSnakeCase("$A"));
        assertEquals("$_a", camelToSnakeCase("$_A"));
    }

}
