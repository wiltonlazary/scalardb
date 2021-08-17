package com.scalar.dataloader.common.service.exports;


import com.scalar.dataloader.common.dao.generic.GenericDao;

public abstract class ExportService {

    protected final GenericDao dao;

    public ExportService(GenericDao dao) {
        this.dao = dao;
    }

    public abstract void export(Export export);
}
