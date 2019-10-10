package br.unb.oss.driver.api.producer;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

public class ProducerFilterImpl<T, U> extends ProducerBase<T> implements Consumer<T>, Producer<T> {

  private final Predicate<T> filter;
  private Consumer<? super T> consumer = null;
  private final ProducerBase<T> parentProducer;

  private final AtomicLong allowed = new AtomicLong(0);
  private final AtomicInteger wip = new AtomicInteger(0);
  private volatile boolean cancelFlag = false;
  private volatile boolean complete = false;

  private volatile T cachedItem = null;
  private volatile Throwable cachedError = null;

  private void drain() {
    if (wip.getAndIncrement() == 0) {
      do {
        pump();
      } while (wip.decrementAndGet() != 0);
    }
  }

  private void pump() {
    if (cancelFlag) {
      return;
    }
    boolean flushed = false;
    Throwable error = cachedError;
    boolean isComplete = complete;
    if (cachedItem != null) {
      if (allowed.get() <= 0) {
        return;
      }
      allowed.decrementAndGet();
      consumer.consume(cachedItem);
      cachedItem = null;
      flushed = true;
    }
    if (error != null) {
      consumer.operationAborted(error);
      cancelFlag = true;
      return;
    }
    if (isComplete) {
      consumer.operationComplete();
      cancelFlag = true;
      return;
    }
    if (flushed) {
      parentProducer.produce(1);
    }
  }

  public ProducerFilterImpl(ProducerBase<T> producer, Predicate<T> filter) {
    this.parentProducer = producer;
    this.filter = filter;
  }

  @Override
  public void consume(T item) {
    // cache is always null when consume is called
    if (cancelFlag) {
      return;
    }
    if (filter.test(item)) {
      cachedItem = item;
      drain();
    } else {
      parentProducer.produce(1);
    }
  }

  @Override
  public void operationComplete() {
    complete = true;
    drain();
  }

  @Override
  public void operationAborted(Throwable error) {
    cachedError = error;
  }

  @Override
  public void register(Consumer<? super T> consumer) {
    if (this.consumer != null) {
      throw new IllegalStateException("Only one consumer is allowed to be registered.");
    }
    this.consumer = consumer;
    parentProducer.register(this);
    parentProducer.produce(1); // we want to look ahead;
  }

  @Override
  public void produce(long n) {
    if (this.consumer == null) {
      throw new IllegalStateException("A consumer should be registered first.");
    }
    if (n <= 0) {
      throw new IllegalArgumentException("You should request more than zero rows.");
    }
    allowed.addAndGet(n);
    getExecutor().execute(this::drain);
  }

  @Override
  public void cancel() {
    if (this.consumer == null) {
      throw new IllegalStateException("A consumer should be registered first.");
    }
    cancelFlag = true;
    parentProducer.cancel();
  }

  @Override
  Executor getExecutor() {
    return parentProducer.getExecutor();
  }
}
