package com.scalar.dataloader.common.service.exports;

import com.scalar.dataloader.common.dao.generic.GenericDao;
import com.scalar.db.api.DistributedTransactionManager;

public class ExportServiceForTransaction extends ExportService {

    private final DistributedTransactionManager transactionManager;

    public ExportServiceForTransaction(GenericDao dao, DistributedTransactionManager transactionManager) {
        super(dao);
        this.transactionManager = transactionManager;
    }

    @Override
    public void export(Export export) {
    }
}
