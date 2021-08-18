package com.scalar.dataloader.common.dao;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionManager;

public interface ScalarDbFactory {
  DistributedStorage createDistributedStorage();
  DistributedTransactionManager createDistributedTransactionManager(DistributedStorage storage);
  DistributedStorageAdmin createDistributedStorageAdmin();
}
