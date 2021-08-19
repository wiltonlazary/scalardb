package com.scalar.dataloader.common.service.exports;

import java.util.ArrayList;
import java.util.List;

public class Export {

  // Required
  private final String namespace;
  private final String tableName;
  private final KeyFilter scanPartitionKey;

  // Optional
  private final String outputFilePath;
  private final List<ScanOrdering> sorts;
  private final List<String> projections;
  private final KeyFilter scanStartKeyFilter;
  private final KeyFilter scanEndKeyFilter;
  private final boolean isScanStartInclusive;
  private final boolean isScanEndInclusive;
  private final int limit;

  private Export(ExportBuilder builder) {
    this.namespace = builder.namespace;
    this.tableName = builder.tableName;
    this.scanPartitionKey = builder.scanPartitionKey;

    this.outputFilePath = builder.outputFilePath;
    this.projections = builder.projections;
    this.sorts = builder.sorts;
    this.scanStartKeyFilter = builder.scanStartKey;
    this.scanEndKeyFilter = builder.scanEndKey;
    this.isScanStartInclusive = builder.isScanStartInclusive;
    this.isScanEndInclusive = builder.isScanEndInclusive;
    this.limit = builder.limit;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getTableName() {
    return tableName;
  }

  public KeyFilter getScanPartitionKey() {
    return scanPartitionKey;
  }

  public List<String> getProjections() {
    return projections == null ? new ArrayList<>() : this.projections;
  }

  public List<ScanOrdering> getSorts() {
    return sorts;
  }

  public String getOutputFilePath() {
    return outputFilePath;
  }

  public int getLimit() {
    return limit;
  }

  public KeyFilter getScanStartKeyFilter() {
    return scanStartKeyFilter;
  }

  public KeyFilter getScanEndKeyFilter() {
    return scanEndKeyFilter;
  }

  public boolean getIsStartScanInclusive() {
    return isScanStartInclusive;
  }

  public boolean getIsEndScanInclusive() {
    return isScanEndInclusive;
  }

  public static class ExportBuilder {
    // Required
    private final String namespace;
    private final String tableName;
    private KeyFilter scanPartitionKey;

    // Optional
    private String outputFilePath = "<TODO change>";
    private List<ScanOrdering> sorts;
    private List<String> projections;
    private KeyFilter scanStartKey;
    private KeyFilter scanEndKey;
    private boolean isScanStartInclusive = true;
    private boolean isScanEndInclusive = true;
    private int limit = 0;

    public ExportBuilder(String namespace, String tableName, KeyFilter scanPartitionKey) {
      this.namespace = namespace;
      this.tableName = tableName;
      this.scanPartitionKey = scanPartitionKey;
    }

    public ExportBuilder outputFilePath(String outputFilePath) {
      this.outputFilePath = outputFilePath;
      return this;
    }

    public ExportBuilder scanPartitionKey(KeyFilter scanPartitionKey) {
      this.scanPartitionKey = scanPartitionKey;
      return this;
    }

    public ExportBuilder sort(ScanOrdering sort) {
      this.sorts.add(sort);
      return this;
    }

    public ExportBuilder sorts(List<ScanOrdering> sorts) {
      this.sorts = sorts;
      return this;
    }

    public ExportBuilder limit(int limit) {
      this.limit = limit;
      return this;
    }

    public ExportBuilder projection(String projection) {
      this.projections.add(projection);
      return this;
    }

    public ExportBuilder projections(List<String> projections) {
      this.projections = projections;
      return this;
    }

    public ExportBuilder scanStartClusteringKey(KeyFilter scanStartClusteringKey) {
      this.scanStartKey = scanStartClusteringKey;
      return this;
    }

    public ExportBuilder scanEndClusteringKey(KeyFilter scanEndClusteringKey) {
      this.scanEndKey = scanEndClusteringKey;
      return this;
    }

    public ExportBuilder isScanStartInclusive(boolean isScanStartInclusive) {
      this.isScanStartInclusive = isScanStartInclusive;
      return this;
    }

    public ExportBuilder isScanEndInclusive(boolean isScanEndInclusive) {
      this.isScanEndInclusive = isScanEndInclusive;
      return this;
    }

    public Export build() {
      return new Export(this);
    }
  }
}
