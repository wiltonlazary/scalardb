package com.scalar.db.storage.cassandra;

public enum CassandraCompactionStrategy {
  STCS("SizeTieredCompactionStrategy"),
  LCS("LeveledCompactionStrategy"),
  TWCS("TimeWindowCompactionStrategy");
  private final String strategyName;

  /** @param strategyName */
  CassandraCompactionStrategy(final String strategyName) {
    this.strategyName = strategyName;
  }

  public static CassandraCompactionStrategy fromString(String text) {
    for (CassandraCompactionStrategy strategy : CassandraCompactionStrategy.values()) {
      if (strategy.strategyName.equalsIgnoreCase(text)) {
        return strategy;
      }
    }
    throw new IllegalArgumentException(
        String.format("The compaction strategy %s does not exist", text));
  }

  /* (non-Javadoc)
   * @see java.lang.Enum#toString()
   */
  @Override
  public String toString() {
    return strategyName;
  }
}
