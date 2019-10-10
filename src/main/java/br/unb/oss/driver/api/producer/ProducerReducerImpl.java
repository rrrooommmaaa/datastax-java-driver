package br.unb.oss.driver.api.producer;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

public class ProducerReducerImpl<T, U> extends ProducerBase<T> implements Consumer<T>, Producer<T> {

    private final BiFunction<T, T, T> reducer;
    private Consumer<? super T> consumer = null;
    private final ProducerBase<T> parentProducer;

    private final AtomicLong allowed = new AtomicLong(0);
    private final AtomicInteger wip = new AtomicInteger(0);

    private volatile T result = null;
    private volatile boolean complete = false;

    private void drain() {
        if (wip.getAndIncrement() == 0) {
            do {
                pump();
            } while (wip.decrementAndGet() != 0);
        }
    }

    private void flushOperationComplete() {
        complete = false;
        consumer.operationComplete();
    }

    private void pump() {
        if (complete) {
            if (result == null) {
                // flush operation complete without waiting for allowed rows
                flushOperationComplete();
            } else if (allowed.get() > 0) {
                consumer.consume(result);
                allowed.decrementAndGet(); // not really necessary
                flushOperationComplete();
            }
        }
    }

    public ProducerReducerImpl(ProducerBase<T> producer, BiFunction<T, T, T> reducer) {
        this.parentProducer = producer;
        this.reducer = reducer;
    }

    @Override
    public void consume(T item) {
        if (result == null) {
            result = item;
        } else {
            result = reducer.apply(result, item);
        }
    }

    @Override
    public void operationComplete() {
        complete = true;
        drain();
    }

    @Override
    public void operationAborted(Throwable error) {
        consumer.operationAborted(error);
    }

    @Override
    public void register(Consumer<? super T> consumer) {
        if (this.consumer != null) {
            throw new IllegalStateException("Only one consumer is allowed to be registered.");
        }
        this.consumer = consumer;
        parentProducer.register(this);
        parentProducer.produce(Long.MAX_VALUE);
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
        parentProducer.cancel();
    }

    @Override
    Executor getExecutor() {
        return parentProducer.getExecutor();
    }
}
