package com.scalar.dataloader.common.service.exports;

public class KeyFilter {

    private final String key;
    private final String value;

    public KeyFilter(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return this.key;
    }

    public String getValue() {
        return this.value;
    }
}
