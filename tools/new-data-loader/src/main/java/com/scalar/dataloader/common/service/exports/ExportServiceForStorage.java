package com.scalar.dataloader.common.service.exports;

import com.scalar.dataloader.common.dao.generic.GenericDao;
import com.scalar.db.api.DistributedStorage;

public class ExportServiceForStorage extends ExportService {

  private final DistributedStorage storage;

  public ExportServiceForStorage(GenericDao dao, DistributedStorage storage) {
    super(dao);
    this.storage = storage;
  }

  @Override
  public void export(Export export) {

    // check if keyspace exists

    // check if table name exists

    // check if keys exists

    // check if sorts are ok

    // get the data

    // export to the actual file
  }
}
