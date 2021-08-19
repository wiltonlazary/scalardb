package com.scalar.dataloader.common.dao.generic;

import com.scalar.db.io.Key;

import java.util.Optional;

public class ScanBoundary {

    private final Optional<Key> startKey;
    private final Optional<Key> endKey;
    private final boolean isStartInclusive;
    private final boolean isEndInclusive;

    public ScanBoundary(Optional<Key> startKey, Optional<Key> endKey, boolean isStartInclusive, boolean isEndInclusive) {
        this.startKey = startKey;
        this.endKey = endKey;
        this.isStartInclusive = isStartInclusive;
        this.isEndInclusive = isEndInclusive;
    }

    public Optional<Key> getStartKey() {
        return this.startKey;
    }

    public Optional<Key> getEndKey() {
        return this.endKey;
    }

    public boolean getIsStartInclusive() {
        return this.isStartInclusive;
    }

    public boolean getIsEndInclusive() {
        return this.isEndInclusive;
    }
}
