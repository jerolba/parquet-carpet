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
package com.jerolba.carpet.impl.read;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class ColumnPath {

    private record Column(Class<?> clazz, String field, String column) {
    }

    private final List<Column> path;
    private final Column head;

    ColumnPath() {
        this.path = new ArrayList<>();
        this.head = null;
    }

    private ColumnPath(List<Column> path, Column head) {
        this.path = path;
        this.head = head;
    }

    public ColumnPath add(Class<?> clazz, String field, String column) {
        ArrayList<Column> appended = new ArrayList<>(path);
        Column head = new Column(clazz, field, column);
        appended.add(head);
        return new ColumnPath(appended, head);
    }

    public String getClassName() {
        return head.clazz.getName();
    }

    public String path() {
        return path.stream().map(c -> c.column).collect(Collectors.joining("."));
    }

}
