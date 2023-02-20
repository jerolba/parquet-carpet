package com.jerolba.carpet;

public class CarpetReadConfiguration {

    private final boolean ignoreUnknown;
    private final boolean strictNumericType;

    public CarpetReadConfiguration(boolean ignoreUnknown, boolean strictNumericType) {
        this.ignoreUnknown = ignoreUnknown;
        this.strictNumericType = strictNumericType;
    }

    public boolean isIgnoreUnknown() {
        return ignoreUnknown;
    }

    public boolean isStrictNumericType() {
        return strictNumericType;
    }

}
