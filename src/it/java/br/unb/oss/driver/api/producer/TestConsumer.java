package br.unb.oss.driver.api.producer;

import com.datastax.oss.driver.api.core.cql.Row;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TestConsumer implements Consumer<Row> {

  private final CountDownLatch terminationLatch = new CountDownLatch(1);
  private final Lock lock = new ReentrantLock();
  private final Condition rowConsumed = lock.newCondition();
  private final List<Row> rows = new ArrayList<>();
  private Throwable error;

  @Override
  public void consume(@NonNull Row row) {
    lock.lock();
    try {
      rows.add(row);
      rowConsumed.signal();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void operationComplete() {
    terminationLatch.countDown();
  }

  @Override
  public void operationAborted(@NonNull Throwable error) {
    this.error = error;
    terminationLatch.countDown();
  }

  public void awaitTermination() throws InterruptedException {
    terminationLatch.await();
  }

  public List<Row> getRows() {
    return rows;
  }

  public Throwable getError() {
    return error;
  }

  public boolean isTerminated() {
    return terminationLatch.getCount() == 0;
  }

  public void awaitProduction(int total) throws InterruptedException {
    lock.lock();
    try {
      while (rows.size() < total) {
        rowConsumed.await();
      }
    } finally {
      lock.unlock();
    }
  }
}
