package com.scalar.dataloader.cli.commands;

import com.google.inject.Inject;
import com.scalar.dataloader.cli.util.TableGenerator;
import com.scalar.dataloader.common.service.exports.ExportService;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import picocli.CommandLine;

import java.util.*;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "meta", description = "list table meta data")
public class MetaCommand implements Callable<Integer> {

  @CommandLine.Spec CommandLine.Model.CommandSpec spec;

  @CommandLine.Option(
      names = {"-ns", "--namespace"},
      paramLabel = "NAMESPACE",
      description = "keyspace to export table data from",
      required = true)
  String namespace;

  @CommandLine.Option(
      names = {"-t", "--tableName"},
      paramLabel = "TABLE_NAME",
      description = "table to export data from",
      required = true)
  String tableName;

  private final ExportService exportService;

  @Inject
  public MetaCommand(ExportService exportService) {
    this.exportService = exportService;
  }

  @Override
  public Integer call() throws Exception {
    TableMetadata tableMetadata = this.exportService.getTableMetadata(namespace, tableName);
    System.out.printf("Table '%s' in namespace '%s':", tableName, namespace);
    System.out.print(this.getFormattedColumns(tableMetadata));
    System.out.print(this.getFormattedIndexes(tableMetadata));
    return 0;
  }

  private String getFormattedColumns(TableMetadata tableMetadata) {
    ArrayList<String> headers = new ArrayList<>();
    List<List<String>> rows = new ArrayList<>();

    headers.add("Column");
    headers.add("Type");
    headers.add("Key");
    headers.add("Clustering order");

    LinkedHashSet<String> columnNames = tableMetadata.getColumnNames();
    LinkedHashSet<String> partitionKeyNames = tableMetadata.getPartitionKeyNames();
    LinkedHashSet<String> clusteringKeyNames = tableMetadata.getClusteringKeyNames();

    for (String columnName : columnNames) {
      List<String> row = new ArrayList<>();

      DataType columnDataType = tableMetadata.getColumnDataType(columnName);

      row.add(columnName);
      row.add(columnDataType.toString());

      // key type column
      if (partitionKeyNames.contains(columnName)) {
        row.add("partition key");
        row.add("");
      } else if (clusteringKeyNames.contains(columnName)) {
        row.add("clustering key");
        Scan.Ordering.Order clusteringOrder = tableMetadata.getClusteringOrder(columnName);
        row.add(clusteringOrder.toString());
      } else {
        row.add("");
        row.add("");
      }
      rows.add(row);
    }
    return new TableGenerator().generateTable(headers, rows);
  }

  private String getFormattedIndexes(TableMetadata tableMetadata) {
    ArrayList<String> headers = new ArrayList<>();
    List<List<String>> rows = new ArrayList<>();

    headers.add("Indexes");

    // Index table
    Set<String> secondaryIndexNames = tableMetadata.getSecondaryIndexNames();
    if (secondaryIndexNames.isEmpty()) {
      return new TableGenerator().generateTable(headers, rows);
    }

    for (String secondaryIndexName : secondaryIndexNames) {
      List<String> row = new ArrayList<>();
      row.add(secondaryIndexName);
      rows.add(row);
    }
    return new TableGenerator().generateTable(headers, rows);
  }
}
