package com.moniepoint.kvstore.entity;

public class WalEntry {
    private String key;
    private String value;
    private boolean delete;

    public WalEntry() {
    }

    public WalEntry(String key, String value) {
        this(key, value, false);
    }

    public WalEntry(String key, String value, boolean delete) {
        this.key = key;
        this.value = value;
        this.delete = delete;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public boolean isDeleted() {
        return delete;
    }
}
