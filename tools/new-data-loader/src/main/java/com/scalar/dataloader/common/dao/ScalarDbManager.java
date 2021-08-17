package com.scalar.dataloader.common.dao;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransactionManager;

public class ScalarDbManager {
    private final DistributedStorage storage;
    private final DistributedTransactionManager transactionManager;

    public ScalarDbManager(ScalarDbFactory scalarDbFactory) {
        storage = scalarDbFactory.createDistributedStorage();
        transactionManager = scalarDbFactory.createDistributedTransactionManager(storage);
    }

    public DistributedStorage getDistributedStorage() {
        return storage;
    }

    public DistributedTransactionManager getDistributedTransactionManager() {
        return transactionManager;
    }
}
