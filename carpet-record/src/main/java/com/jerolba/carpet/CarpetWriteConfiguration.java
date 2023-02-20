package com.jerolba.carpet;

public class CarpetWriteConfiguration {

    private final AnnotatedLevels annotatedLevels;

    public CarpetWriteConfiguration(AnnotatedLevels annotatedLevels) {
        this.annotatedLevels = annotatedLevels;
    }

    public AnnotatedLevels annotatedLevels() {
        return annotatedLevels;
    }

}
