package com.scalar.dataloader.common.service.exports;

import com.scalar.dataloader.common.service.KeyFilter;
import com.scalar.dataloader.common.service.OutputFormat;

import java.util.List;

public class Export {

  // Required
  private final String namespace;
  private final String tableName;
  private final KeyFilter scanPartitionKey;

  // Optional
  private final String outputFilePath;
  private final OutputFormat outputFormat;
  private final List<ScanOrdering> sorts;
  private final List<String> columns;
  private final KeyFilter scanStartClusteringKeyFilter;
  private final KeyFilter scanEndClusteringKeyFilter;

  private Export(ExportBuilder builder) {
    this.namespace = builder.namespace;
    this.tableName = builder.tableName;
    this.scanPartitionKey = builder.scanPartitionKey;

    this.outputFilePath = builder.outputFilePath;
    this.outputFormat = builder.outputFormat;
    this.columns = builder.columns;
    this.sorts = builder.sorts;
    this.scanStartClusteringKeyFilter = builder.scanStartClusteringKey;
    this.scanEndClusteringKeyFilter = builder.scanEndClusteringKey;
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

  public List<String> getColumns() {
    return columns;
  }

  public List<ScanOrdering> getSorts() {
    return sorts;
  }

  public String getOutputFilePath() {
    return outputFilePath;
  }

  public OutputFormat getOutputFormat() {
    return outputFormat;
  }

  public KeyFilter getScanStartClusteringKeyFilter() {
    return scanStartClusteringKeyFilter;
  }

  public KeyFilter getScanEndClusteringKeyFilter() {
    return scanEndClusteringKeyFilter;
  }

  public static class ExportBuilder {
    // Required
    private final String namespace;
    private final String tableName;
    private KeyFilter scanPartitionKey;

    // Optional
    private String outputFilePath = "<TODO change>";
    private OutputFormat outputFormat = OutputFormat.JSON;
    private List<ScanOrdering> sorts;
    private List<String> columns;
    private KeyFilter scanStartClusteringKey;
    private KeyFilter scanEndClusteringKey;

    public ExportBuilder(String namespace, String tableName, KeyFilter scanPartitionKey) {
      this.namespace = namespace;
      this.tableName = tableName;
      this.scanPartitionKey = scanPartitionKey;
    }

    public ExportBuilder outputFilePath(String outputFilePath) {
      this.outputFilePath = outputFilePath;
      return this;
    }

    public ExportBuilder outputFormat(OutputFormat outputFormat) {
      this.outputFormat = outputFormat;
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

    public ExportBuilder column(String column) {
      this.columns.add(column);
      return this;
    }

    public ExportBuilder columns(List<String> columns) {
      this.columns = columns;
      return this;
    }

    public ExportBuilder scanStartClusteringKey(KeyFilter scanStartClusteringKey) {
      this.scanStartClusteringKey = scanStartClusteringKey;
      return this;
    }

    public ExportBuilder scanEndClusteringKey(KeyFilter scanEndClusteringKey) {
      this.scanEndClusteringKey = scanEndClusteringKey;
      return this;
    }

    public Export build() {
      return new Export(this);
    }
  }
}
