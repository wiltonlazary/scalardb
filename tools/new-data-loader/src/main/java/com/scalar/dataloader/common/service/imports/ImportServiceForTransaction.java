package com.scalar.dataloader.common.service.imports;

import com.scalar.dataloader.common.dao.generic.GenericDao;
import com.scalar.db.api.DistributedTransactionManager;

public class ImportServiceForTransaction extends ImportService {

  private final DistributedTransactionManager transactionManager;

  public ImportServiceForTransaction(GenericDao dao, DistributedTransactionManager transactionManager) {
    super(dao);
    this.transactionManager = transactionManager;
  }
}
