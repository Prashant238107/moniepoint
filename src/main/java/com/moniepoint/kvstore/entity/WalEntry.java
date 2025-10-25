package com.moniepoint.kvstore.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class WalEntry {
    private String key;
    private String value;
    private boolean delete;

    public WalEntry() {
    }

    public WalEntry(String key, String value) {
        this(key, value, false);
    }

    @JsonCreator
    public WalEntry(
            @JsonProperty("key") String key,
            @JsonProperty("value") String value,
            @JsonProperty("delete") boolean delete
    ) {
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

    @JsonProperty("delete") // Ensure the JSON field is named "delete" during serialization
    public boolean isDeleted() {
        return delete;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setDelete(boolean delete) {
        this.delete = delete;
    }
}
