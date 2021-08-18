package com.scalar.dataloader.common.dao.generic;

import com.scalar.dataloader.common.dao.DaoException;
import com.scalar.dataloader.common.service.exports.ScanOrdering;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

public class GenericDao {

  private final Logger log = LoggerFactory.getLogger(this.getClass());

  public void scan(
      String namespace,
      String tableName,
      Key partitionKey,
      Optional<Key> startKey,
      Optional<Key> endKey,
      List<ScanOrdering> sorts,
      DistributedStorage storage)
      throws DaoException {
    // setup scan
    Scan scan = new Scan(partitionKey).forNamespace(namespace).forTable(tableName);

    // with start
    if (startKey != null && startKey.isPresent()) {
      scan.withStart(startKey.get());
    }

    // with end
    if (endKey != null && endKey.isPresent()) {
      scan.withEnd(endKey.get());
    }

    // clustering order
    for (ScanOrdering sort : sorts) {
      scan.withOrdering(new Scan.Ordering(sort.getClusteringKey(), sort.getSortOrder()));
    }

    // scan and parse data
    try {
      Scanner scanner = storage.scan(scan);
      for (Result result : scanner) {
        System.out.println(result);
      }
    } catch (ExecutionException e) {
      throw new DaoException("error SCAN " + "fefefee", e);
    }
    //    log.info("SCAN completed for " + accountId);
  }
}
