package com.scalar.db.service;

import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import java.util.Map;

public class AdminService implements DistributedStorageAdmin {

  private final DistributedStorageAdmin admin;

  @Inject
  public AdminService(DistributedStorageAdmin admin) {
    this.admin = admin;
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options) {
    admin.createTable(namespace, table, metadata, options);
  }

  @Override
  public void dropTable(String namespace, String table) {
    admin.dropTable(namespace, table);
  }

  @Override
  public void truncateTable(String namespace, String table) {
    admin.truncateTable(namespace, table);
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) {
    return admin.getTableMetadata(namespace, table);
  }

  @Override
  public void close() {
    admin.close();
  }
}