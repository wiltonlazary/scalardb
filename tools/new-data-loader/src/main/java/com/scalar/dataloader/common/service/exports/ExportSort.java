package com.scalar.dataloader.common.service.exports;

public class ExportSort {

    private final String clusteringKey;
    private final String sortOrder;

    public ExportSort(String clusteringKey, String sortOrder) {
        this.clusteringKey  = clusteringKey;
        this.sortOrder = sortOrder;
    }

    public String getClusteringKey() {
        return this.clusteringKey;
    }

    public String getSortOrder() {
        return this.sortOrder;
    }
}
