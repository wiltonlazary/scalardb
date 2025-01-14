package com.scalar.db.transaction.consensuscommit;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.db.api.Consistency;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.CrudRuntimeException;
import com.scalar.db.exception.transaction.UncommittedRecordException;
import com.scalar.db.io.Key;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class CrudHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(CrudHandler.class);
  private final DistributedStorage storage;
  private final Snapshot snapshot;

  public CrudHandler(DistributedStorage storage, Snapshot snapshot) {
    this.storage = checkNotNull(storage);
    this.snapshot = checkNotNull(snapshot);
  }

  public Optional<Result> get(Get get) throws CrudException {
    Optional<TransactionResult> result;
    Snapshot.Key key = new Snapshot.Key(get);

    if (snapshot.containsKey(key)) {
      return snapshot.get(key).map(r -> r);
    }

    result = getFromStorage(get);
    if (!result.isPresent()) {
      snapshot.put(key, result);
      return Optional.empty();
    }

    if (result.get().isCommitted()) {
      snapshot.put(key, result);
      return Optional.of(result.get());
    }
    throw new UncommittedRecordException(result.get(), "this record needs recovery");
  }

  public List<Result> scan(Scan scan) throws CrudException {
    List<Result> results = new ArrayList<>();

    Optional<List<Snapshot.Key>> keysInSnapshot = snapshot.get(scan);
    if (keysInSnapshot.isPresent()) {
      keysInSnapshot.get().forEach(key -> snapshot.get(key).ifPresent(results::add));
      return results;
    }

    List<Snapshot.Key> keys = new ArrayList<>();
    Scanner scanner = null;
    try {
      scanner = getFromStorage(scan);
      for (Result r : scanner) {
        TransactionResult result = new TransactionResult(r);
        if (!result.isCommitted()) {
          throw new UncommittedRecordException(result, "the record needs recovery");
        }

        Snapshot.Key key =
            getSnapshotKey(r, scan)
                .orElseThrow(() -> new CrudRuntimeException("can't get a snapshot key"));

        if (snapshot.containsKey(key)) {
          result = snapshot.get(key).orElse(null);
        }

        snapshot.put(key, Optional.ofNullable(result));
        keys.add(key);
        results.add(result);
      }
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException e) {
          LOGGER.warn("failed to close the scanner", e);
        }
      }
    }
    snapshot.put(scan, Optional.of(keys));

    return results;
  }

  public void put(Put put) {
    snapshot.put(new Snapshot.Key(put), put);
  }

  public void delete(Delete delete) {
    snapshot.put(new Snapshot.Key(delete), delete);
  }

  private Optional<TransactionResult> getFromStorage(Get get) throws CrudException {
    try {
      get.withConsistency(Consistency.LINEARIZABLE);
      return storage.get(get).map(TransactionResult::new);
    } catch (ExecutionException e) {
      throw new CrudException("get failed.", e);
    }
  }

  private Scanner getFromStorage(Scan scan) throws CrudException {
    try {
      scan.withConsistency(Consistency.LINEARIZABLE);
      return storage.scan(scan);
    } catch (ExecutionException e) {
      throw new CrudException("scan failed.", e);
    }
  }

  private Optional<Snapshot.Key> getSnapshotKey(Result result, Scan scan) {
    Optional<Key> partitionKey = result.getPartitionKey();
    Optional<Key> clusteringKey = result.getClusteringKey();
    if (!partitionKey.isPresent() || !clusteringKey.isPresent()) {
      return Optional.empty();
    }

    return Optional.of(
        new Snapshot.Key(
            scan.forNamespace().get(),
            scan.forTable().get(),
            partitionKey.get(),
            clusteringKey.get()));
  }

  public Snapshot getSnapshot() {
    return snapshot;
  }
}
