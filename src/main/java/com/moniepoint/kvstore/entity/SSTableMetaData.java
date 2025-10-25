package com.moniepoint.kvstore.entity;

import java.nio.file.Path;

public class SSTableMetaData implements Comparable<SSTableMetaData> {
    private final Path path;
    private final String minKey;
    private final String maxKey;

    public SSTableMetaData(Path path, String minKey, String maxKey) {
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
    public int compareTo(SSTableMetaData other) {
        return other.path.compareTo(this.path);
    }
}