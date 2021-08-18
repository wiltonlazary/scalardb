package com.scalar.dataloader.common.service.imports;

import com.scalar.dataloader.common.dao.generic.GenericDao;

public abstract class ImportService {

  protected final GenericDao dao;

  public ImportService(GenericDao dao) {
    this.dao = dao;
  }
}
