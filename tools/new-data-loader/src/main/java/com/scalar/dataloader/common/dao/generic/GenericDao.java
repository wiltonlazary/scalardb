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

public class GenericDao {

  private final Logger log = LoggerFactory.getLogger(this.getClass());

  public List<Result> scan(
      String namespace,
      String tableName,
      Key partitionKey,
      ScanBoundary scanBoundary,
      List<ScanOrdering> sorts,
      List<String> projections,
      DistributedStorage storage)
      throws DaoException {
    // setup scan
    Scan scan = new Scan(partitionKey).forNamespace(namespace).forTable(tableName);

    // Set boundary start
    if (scanBoundary.getStartKey().isPresent()) {
      scan.withStart(scanBoundary.getStartKey().get(), scanBoundary.getIsStartInclusive());
    }

    // with end
    if (scanBoundary.getEndKey().isPresent()) {
      scan.withEnd(scanBoundary.getEndKey().get(), scanBoundary.getIsEndInclusive());
    }

    // clustering order
    for (ScanOrdering sort : sorts) {
      scan.withOrdering(new Scan.Ordering(sort.getClusteringKey(), sort.getSortOrder()));
    }

    // projections
    if (projections != null && !projections.isEmpty()) {
      scan.withProjections(projections);
    }

    // scan data
    try {
      Scanner scanner = storage.scan(scan);
      return scanner.all();
    } catch (ExecutionException e) {
      throw new DaoException("error SCAN " + "fefefee", e);
    }
    //    log.info("SCAN completed for " + accountId);
  }
}
