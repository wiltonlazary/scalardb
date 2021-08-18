package com.scalar.dataloader.common.service.exports;

import com.scalar.dataloader.common.dao.generic.GenericDao;
import com.scalar.dataloader.common.service.BaseService;
import com.scalar.dataloader.common.service.KeyFilter;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;

import java.util.LinkedHashSet;
import java.util.List;

public abstract class ExportService extends BaseService {

  protected final GenericDao dao;

  public ExportService(GenericDao dao, DistributedStorageAdmin storageAdmin) {
    super(storageAdmin);
    this.dao = dao;
  }

  public abstract void export(Export export) throws Exception;

  protected void validateScalarDBKey(LinkedHashSet<String> partitionKeyNames, KeyFilter key) throws Exception {
    if (!partitionKeyNames.contains(key.getColumn())) {
      throw new Exception(String.format("The key '%s' could not be found", key.getColumn()));
    }
  }

  protected void validateSorts(TableMetadata tableMetadata, List<ScanOrdering> sorts) throws Exception {
    LinkedHashSet<String> clusteringKeyNames = tableMetadata.getClusteringKeyNames();
    for (ScanOrdering sort : sorts) {
      if (!clusteringKeyNames.contains(sort.getClusteringKey())) {
        throw new Exception(String.format("Sorting on clustering key '%s' is not possible, key not found", sort.getClusteringKey()));
      }
    }
  }
}
