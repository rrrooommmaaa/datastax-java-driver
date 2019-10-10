package br.unb.oss.driver.api.producer;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
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
    
    private volatile T last = null;
    
    private void drain() {
        if (wip.getAndIncrement() == 0) {
            do {
                pump();
            } while (wip.decrementAndGet() != 0);
        }
    }

    private void pump() {
        if (!cancelFlag) {
        }
    }

    public ProducerFilterImpl(ProducerBase<T> producer, Predicate<T> filter) {
        this.parentProducer = producer;
        this.filter = filter;
    }

    @Override
    public void consume(T item) {
        if (cancelFlag) {
            return;
        }
        if (filter.test(item)) {
            // last is always null at this point
            last = item;
            drain();
        } else {
            parentProducer.produce(1);
        }
    }

    @Override
    public void operationComplete() {
        consumer.operationComplete();
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
