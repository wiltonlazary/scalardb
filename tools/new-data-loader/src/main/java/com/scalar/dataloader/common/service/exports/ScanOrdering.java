package com.scalar.dataloader.common.service.exports;

import com.scalar.db.api.Scan;

public class ScanOrdering {

  private final String clusteringKey;
  private final Scan.Ordering.Order sortOrder;

  public ScanOrdering(String clusteringKeyColumn, Scan.Ordering.Order sortOrder) {
    this.clusteringKey = clusteringKeyColumn;
    this.sortOrder = sortOrder;
  }

  public String getClusteringKey() {
    return this.clusteringKey;
  }

  public Scan.Ordering.Order getSortOrder() {
    return this.sortOrder;
  }
}
