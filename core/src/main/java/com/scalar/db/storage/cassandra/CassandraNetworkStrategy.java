package com.scalar.db.storage.cassandra;

public enum CassandraNetworkStrategy {
  SIMPLE_STRATEGY("SimpleStrategy"),
  NETWORK_TOPOLOGY_STRATEGY("NetworkTopologyStrategy");
  private final String strategyName;

  /** @param strategyName */
  CassandraNetworkStrategy(final String strategyName) {
    this.strategyName = strategyName;
  }

  public static CassandraNetworkStrategy fromString(String text) {
    for (CassandraNetworkStrategy strategy : CassandraNetworkStrategy.values()) {
      if (strategy.strategyName.equalsIgnoreCase(text)) {
        return strategy;
      }
    }
    throw new IllegalArgumentException(
        String.format("The network strategy %s does not exist", text));
  }

  /* (non-Javadoc)
   * @see java.lang.Enum#toString()
   */
  @Override
  public String toString() {
    return strategyName;
  }
}
