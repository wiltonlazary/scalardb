package com.scalar.dataloader.common.service;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;

public abstract class BaseService {
  protected final DistributedStorageAdmin storageAdmin;

  public BaseService(DistributedStorageAdmin storageAdmin) {
    this.storageAdmin = storageAdmin;
  }

  protected TableMetadata getTableMetadata(String namespace, String tableName) throws Exception {
    TableMetadata tableMetadata = this.storageAdmin.getTableMetadata(namespace, tableName);
    if (tableMetadata == null) {
      throw new Exception(String.format("The provided namespace '%s' and/or table name '%s' is incorrect and could not be found", namespace, tableName));
    }
    return tableMetadata;
  }
}
