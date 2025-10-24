package com.moniepoint.kvstore.service;

import com.moniepoint.kvstore.comparator.NaturalKeyComparator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

@Component
public class MemTable {

    private final ConcurrentNavigableMap<String, String> memtable;
    private long sizeInBytes = 0;

    public static final String DELETED_MARKER = "__DELETED__";

    public MemTable() {
        this.memtable = new ConcurrentSkipListMap<>(new NaturalKeyComparator());
    }

    public void put(String key, String value) {
        String oldValue = memtable.put(key, value);
        if (oldValue == null) { // New entry
            sizeInBytes += key.length() + value.length();
        } else { // Update existing entry
            sizeInBytes += value.length() - oldValue.length();
        }
    }

    public String get(String key) {
        return memtable.get(key);
    }

    public boolean containsKey(String key) {
        return memtable.containsKey(key);
    }

    public ConcurrentNavigableMap<String, String> subMap(String startKey, boolean startInclusive, String endKey, boolean endInclusive) {
        return memtable.subMap(startKey, startInclusive, endKey, endInclusive);
    }

    public void clear() {
        memtable.clear();
        sizeInBytes = 0;
    }

    public boolean isEmpty() {
        return memtable.isEmpty();
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    public Map<String, String> getEntries() {
        return memtable;
    }
}
