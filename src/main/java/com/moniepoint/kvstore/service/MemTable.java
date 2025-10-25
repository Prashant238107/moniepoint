package com.moniepoint.kvstore.service;

import com.moniepoint.kvstore.comparator.NaturalKeyComparator;
import org.springframework.stereotype.Component;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

@Component
public class MemTable {

    private static final Logger logger = LoggerFactory.getLogger(MemTable.class);

    private final ConcurrentNavigableMap<String, String> memtable;
    private long sizeInBytes = 0;

    public MemTable() {
        this.memtable = new ConcurrentSkipListMap<>(new NaturalKeyComparator());
    }

    private int getByteSize(String str) {
        return str.getBytes(StandardCharsets.UTF_8).length;
    }

    public String put(String key, String value) {
        logger.debug("MemTable: Putting key '{}'", key);
        String oldValue = memtable.put(key, value);
        int valueSize = getByteSize(value);

        if (oldValue == null) {
            logger.debug("MemTable: New key '{}', size increased by {} bytes", key, getByteSize(key) + valueSize);
            sizeInBytes += getByteSize(key) + valueSize;
        } else {
            sizeInBytes += valueSize - getByteSize(oldValue);
        }
        return oldValue;
    }

    public String get(String key) {
        logger.debug("MemTable: Getting key '{}'", key);
        return memtable.get(key);
    }

    public boolean containsKey(String key) {
        logger.debug("MemTable: Checking containsKey for '{}'", key);
        return memtable.containsKey(key);
    }

    public ConcurrentNavigableMap<String, String> subMap(String startKey, boolean startInclusive, String endKey, boolean endInclusive) {
        return memtable.subMap(startKey, startInclusive, endKey, endInclusive);
    }

    public void clear() {
        logger.info("MemTable: Clearing all entries.");
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
