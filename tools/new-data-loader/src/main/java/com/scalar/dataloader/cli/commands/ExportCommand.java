package com.scalar.dataloader.cli.commands;

import com.google.inject.Inject;
import com.scalar.dataloader.common.service.OutputFormat;
import com.scalar.dataloader.common.service.exports.Export;
import com.scalar.dataloader.common.service.exports.ExportService;
import com.scalar.dataloader.common.service.exports.ExportSort;
import com.scalar.dataloader.common.service.exports.KeyFilter;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

@CommandLine.Command(
        name = "export",
        description = "export table data"
)
public class ExportCommand implements Callable<Integer> {

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    @CommandLine.Option(names = {"-ks", "--keyspace"}, paramLabel = "KEYSPACE", description = "keyspace to export table data from", required = true)
    String keyspace;

    @CommandLine.Option(names = {"-t", "--tableName"}, paramLabel = "TABLE_NAME", description = "table to export data from", required = true)
    String tableName;

    @CommandLine.Option(names = {"-k", "--key"}, paramLabel = "KEY", description = "partition or clustering key", required = true)
    String[] keys;

    @CommandLine.Option(names = {"-s", "--sort"}, paramLabel = "SORT", description = "clustering key sorting order")
    String[] sorts;

    @CommandLine.Option(names = {"-cols", "--columns"}, paramLabel = "COLUMNS", description = "columns to export")
    String[] columns;

    @CommandLine.Option(names = {"-o", "--outputFile"}, paramLabel = "OUTPUT_FILE", description = "output file path")
    String outputFilePath;

    @CommandLine.Option(names = {"-f", "--format"}, paramLabel = "FORMAT", description = "ouput date file format")
    String outputFormat;

    private final ExportService exportService;

    @Inject
    public ExportCommand(ExportService exportService) {
        this.exportService = exportService;
    }

    @Override
    public Integer call() throws Exception {
        List<KeyFilter> keyFilters = this.parseKeyFilters(this.keys);
        List<ExportSort> sorts = this.parseSort(this.sorts);

        Export.ExportBuilder builder = new Export.ExportBuilder(this.keyspace, this.tableName, keyFilters).sorts(sorts);
        if (this.columns != null && this.columns.length > 0) {
            builder = builder.columns(Arrays.asList(columns));
        }
        if (this.outputFilePath != null) {
            builder = builder.outputFilePath(this.outputFilePath);
        }
        if (this.outputFormat != null) {
            builder = builder.outputFormat(OutputFormat.valueOf(this.outputFormat));
        }

        this.exportService.export(builder.build());

        return 0;
    }

    private List<KeyFilter> parseKeyFilters(String[] keys) throws Exception {
        List<KeyFilter> keyFilters = new ArrayList<>();
        Pattern pattern = Pattern.compile(".*=.*", Pattern.CASE_INSENSITIVE);
        for (String keyFilter : keys) {
            // pattern checking
            if (!pattern.matcher(keyFilter.toLowerCase()).matches()) {
                throw new Exception(String.format("They provided key '%s is not formatted correctly. Expected format is field=value.", keyFilter));
            }
            String[] split = keyFilter.split("=");
            keyFilters.add(new KeyFilter(split[0], split[1]));
        }
        return keyFilters;
    }

    private List<ExportSort> parseSort(String[] sorts) throws Exception {
        List<ExportSort> exportSorts = new ArrayList<>();
        if (sorts == null || sorts.length == 0) {
            return exportSorts;
        }

        Pattern pattern = Pattern.compile(".*=.*", Pattern.CASE_INSENSITIVE);
        for (String sort : sorts) {
            // pattern checking
            if (!pattern.matcher(sort.toLowerCase()).matches()) {
                throw new Exception(String.format("They provided sort '%s is not formatted correctly. Expected format is field=asc|desc.", sort));
            }

            // validate ASC and DESC
            String[] split = sort.toLowerCase().split("=");
            if (!split[1].equals("asc") && !split[1].equals("desc")) {
                throw new Exception(String.format("They provided sort '%s is not formatted correctly. Expected format is field=asc|desc.", sort));
            }
            exportSorts.add(new ExportSort(split[0], split[1]));
        }
        return exportSorts;
    }
}

