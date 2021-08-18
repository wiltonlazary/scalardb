package com.scalar.dataloader.common.service.imports;

import com.scalar.dataloader.common.dao.generic.GenericDao;
import com.scalar.db.api.DistributedStorage;

public class ImportServiceForStorage extends ImportService {

  private final DistributedStorage storage;

  public ImportServiceForStorage(GenericDao dao, DistributedStorage storage) {
    super(dao);
    this.storage = storage;
  }
}
