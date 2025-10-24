package com.moniepoint.kvstore.service;

import com.moniepoint.kvstore.comparator.NaturalKeyComparator;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

@Component
public class MemTable {

    private final ConcurrentNavigableMap<String, String> memtable;
    private long sizeInBytes = 0;

    public MemTable() {
        this.memtable = new ConcurrentSkipListMap<>(new NaturalKeyComparator());
    }

    private int getByteSize(String str) {
        return str.getBytes(StandardCharsets.UTF_8).length;
    }

    public void put(String key, String value) {
        String oldValue = memtable.put(key, value);
        int valueSize = getByteSize(value);

        if (oldValue == null) {
            sizeInBytes += getByteSize(key) + valueSize;
        } else {
            sizeInBytes += valueSize - getByteSize(oldValue);
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

    public ConcurrentNavigableMap<String, String> getEntries() {
        return memtable;
    }
}
