package com.jerolba.carpet.impl.read;

import java.util.ArrayList;
import java.util.List;

class ListHolder {

    public List<Object> list;

    public void start() {
        list = new ArrayList<>();
    }

    public Object end() {
        return list;
    }

    public void add(Object value) {
        this.list.add(value);
    }

}