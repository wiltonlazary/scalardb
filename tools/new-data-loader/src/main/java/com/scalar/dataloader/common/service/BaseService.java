package com.scalar.dataloader.common.service;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;

public abstract class BaseService {
  protected final DistributedStorageAdmin storageAdmin;

  public BaseService(DistributedStorageAdmin storageAdmin) {
    this.storageAdmin = storageAdmin;
  }

  public TableMetadata getTableMetadata(String namespace, String tableName) throws Exception {
    TableMetadata tableMetadata = this.storageAdmin.getTableMetadata(namespace, tableName);
    if (tableMetadata == null) {
      throw new Exception(
          String.format(
              "The provided namespace '%s' and/or table name '%s' is incorrect and could not be found",
              namespace, tableName));
    }
    return tableMetadata;
  }

  protected Map<String, DataType> getColumnDataTypes(String namespace, String tableName)
      throws Exception {

    TableMetadata tableMetadata = this.getTableMetadata(namespace, tableName);

    Map<String, DataType> definitions = new HashMap<>();
    for (String columnName : tableMetadata.getColumnNames()) {
      definitions.put(columnName, tableMetadata.getColumnDataType(columnName));
    }

    return definitions;
  }

  protected void validateScalarDBKey(LinkedHashSet<String> partitionKeyNames, KeyFilter key)
      throws Exception {
    if (!partitionKeyNames.contains(key.getColumn())) {
      throw new Exception(String.format("The key '%s' could not be found", key.getColumn()));
    }
  }
}
