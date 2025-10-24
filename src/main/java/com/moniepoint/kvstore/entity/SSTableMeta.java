package com.moniepoint.kvstore.entity;

import java.nio.file.Path;

public class SSTableMeta implements Comparable<SSTableMeta> {
    private final Path path;
    private final String minKey;
    private final String maxKey;

    public SSTableMeta(Path path, String minKey, String maxKey) {
        this.path = path;
        this.minKey = minKey;
        this.maxKey = maxKey;
    }

    public Path getPath() {
        return path;
    }

    public String getMinKey() {
        return minKey;
    }

    public String getMaxKey() {
        return maxKey;
    }

    @Override
    public int compareTo(SSTableMeta other) {
        return other.path.compareTo(this.path);
    }
}