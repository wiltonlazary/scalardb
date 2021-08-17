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
    }
}
