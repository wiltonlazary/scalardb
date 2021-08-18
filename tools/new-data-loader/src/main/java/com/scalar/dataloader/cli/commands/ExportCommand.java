package com.scalar.dataloader.cli.commands;

import com.google.inject.Inject;
import com.scalar.dataloader.common.service.OutputFormat;
import com.scalar.dataloader.common.service.exports.Export;
import com.scalar.dataloader.common.service.exports.ExportService;
import com.scalar.dataloader.common.service.exports.ScanOrdering;
import com.scalar.dataloader.common.service.KeyFilter;
import com.scalar.db.api.Scan;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

@CommandLine.Command(name = "export", description = "export table data")
public class ExportCommand implements Callable<Integer> {

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

  @CommandLine.Option(
      names = {"-k", "--key"},
      paramLabel = "KEY",
      description = "partition key for scan",
      required = true)
  String partitionKey;

  @CommandLine.Option(
          names = {"-s", "--start"},
          paramLabel = "KEY",
          description = "clustering key to mark scan start",
          required = false)
  String scanStartClusteringKey;

  @CommandLine.Option(
          names = {"-e", "--end"},
          paramLabel = "KEY",
          description = "clustering key to mark scan end",
          required = false)
  String scanEndClusteringKey;

  @CommandLine.Option(
      names = {"-so", "--sort"},
      paramLabel = "SORT",
      description = "clustering key sorting order")
  String[] sorts;

  @CommandLine.Option(
      names = {"-cols", "--columns"},
      paramLabel = "COLUMNS",
      description = "columns to export")
  String[] columns;

  @CommandLine.Option(
      names = {"-o", "--outputFile"},
      paramLabel = "OUTPUT_FILE",
      description = "output file path")
  String outputFilePath;

  @CommandLine.Option(
      names = {"-f", "--format"},
      paramLabel = "FORMAT",
      description = "ouput date file format")
  String outputFormat;

  private final ExportService exportService;

  @Inject
  public ExportCommand(ExportService exportService) {
    this.exportService = exportService;
  }

  @Override
  public Integer call() throws Exception {
    KeyFilter scanPartitionKey = this.parseKey(this.partitionKey);
    List<ScanOrdering> sorts = this.parseSort(this.sorts);

    Export.ExportBuilder builder =
        new Export.ExportBuilder(this.namespace, this.tableName, scanPartitionKey).sorts(sorts);
    if (this.columns != null && this.columns.length > 0) {
      builder = builder.columns(Arrays.asList(columns));
    }
    if (this.outputFilePath != null) {
      builder = builder.outputFilePath(this.outputFilePath);
    }
    if (this.outputFormat != null) {
      builder = builder.outputFormat(OutputFormat.valueOf(this.outputFormat));
    }
    if (this.scanStartClusteringKey != null) {
      builder = builder.scanStartClusteringKey(this.parseKey(this.scanStartClusteringKey));
    }
    if (this.scanEndClusteringKey != null) {
      builder = builder.scanEndClusteringKey(this.parseKey(this.scanEndClusteringKey));
    }

    this.exportService.export(builder.build());

    return 0;
  }

  private KeyFilter parseKey(String key) throws Exception {
    Pattern pattern = Pattern.compile(".*=.*", Pattern.CASE_INSENSITIVE);
    // pattern checking
    if (!pattern.matcher(key.toLowerCase()).matches()) {
      throw new Exception(
          String.format(
              "They provided key '%s is not formatted correctly. Expected format is field=value.",
              key));
    }
    String[] split = key.split("=");
    return new KeyFilter(split[0], split[1]);
  }

  private List<ScanOrdering> parseSort(String[] sorts) throws Exception {
    List<ScanOrdering> exportSorts = new ArrayList<>();
    if (sorts == null || sorts.length == 0) {
      return exportSorts;
    }

    Pattern pattern = Pattern.compile(".*=.*", Pattern.CASE_INSENSITIVE);
    for (String sort : sorts) {
      // pattern checking and ASC|DESC check
      String[] split = sort.toLowerCase().split("=");
      if (!pattern.matcher(sort.toLowerCase()).matches() || (!split[1].equals("asc") && !split[1].equals("desc"))) {
        throw new Exception(
            String.format(
                "They provided sort '%s is not formatted correctly. Expected format is field=asc|desc.",
                sort));
      }
      exportSorts.add(
          new ScanOrdering(split[0], Scan.Ordering.Order.valueOf(split[1].toUpperCase())));
    }
    return exportSorts;
  }
}
