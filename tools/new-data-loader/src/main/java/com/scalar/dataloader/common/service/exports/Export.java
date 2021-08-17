package com.scalar.dataloader.common.service.exports;

import com.scalar.dataloader.common.service.OutputFormat;

import java.util.List;

public class Export {

    // Required
    private final String keyspace;
    private final String tableName;
    private final List<KeyFilter> keyFilters;

    // Optional
    private final String outputFilePath;
    private final OutputFormat outputFormat;
    private final List<ExportSort> sorts;
    private final List<String> columns;

    private Export(ExportBuilder builder) {
        this.keyspace = builder.keyspace;
        this.tableName = builder.tableName;
        this.keyFilters = builder.keyFilters;

        this.outputFilePath = builder.outputFilePath;
        this.outputFormat = builder.outputFormat;
        this.columns = builder.columns;
        this.sorts = builder.sorts;
    }

    public String getKeyspace() {
        return keyspace;
    }
    public String getTableName() {
        return tableName;
    }
    public List<KeyFilter> getKeyFilters() {
        return keyFilters;
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<ExportSort> getSorts() {
        return sorts;
    }

    public String getOutputFilePath() {
        return outputFilePath;
    }

    public OutputFormat getOutputFormat() {
        return outputFormat;
    }

    public static class ExportBuilder {
        // Required
        private final String keyspace;
        private final String tableName;
        private List<KeyFilter> keyFilters;

        // Optional
        private String outputFilePath = "<TODO change>";
        private OutputFormat outputFormat = OutputFormat.JSON;
        private List<ExportSort> sorts;
        private List<String> columns;

        public ExportBuilder(String keyspace, String tableName, List<KeyFilter> keyFilters) {
            this.keyspace = keyspace;
            this.tableName = tableName;
            this.keyFilters = keyFilters;
        }

        public ExportBuilder outputFilePath(String outputFilePath) {
            this.outputFilePath = outputFilePath;
            return this;
        }

        public ExportBuilder outputFormat(OutputFormat outputFormat) {
            this.outputFormat = outputFormat;
            return this;
        }

        public ExportBuilder keyFilter(KeyFilter keyFilter) {
            this.keyFilters.add(keyFilter);
            return this;
        }

        public ExportBuilder keyFilters(List<KeyFilter> keyFilters) {
            this.keyFilters = keyFilters;
            return this;
        }

        public ExportBuilder sort(ExportSort sort) {
            this.sorts.add(sort);
            return this;
        }

        public ExportBuilder sorts(List<ExportSort> sorts) {
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

        public Export build() {
            return new Export(this);
        }
    }
}