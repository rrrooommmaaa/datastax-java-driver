package br.unb.oss.driver.api.producer;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ProducerImpl<U> extends ProducerBase<Row> implements Producer<Row> {

    private final CompletionStage<AsyncResultSet> stage;
    private volatile Consumer consumer = null;
    private final AtomicLong allowed = new AtomicLong(0);
    private final AtomicInteger wip = new AtomicInteger(0);
    private volatile boolean cancelFlag = false;
    private volatile AsyncResultSet currentState;

    private Void onError(Throwable error) {
        sendOperationAborted(error.getCause());
        return null;
    }

    private static Executor executor = Executors.newCachedThreadPool();

    private void drain() {
        if (wip.getAndIncrement() == 0) {
            do {
                pump();
            } while (wip.decrementAndGet() != 0);
        }
    }

    private void pump() {
        if (currentState == null) {
            return;
        }
        while (!cancelFlag && (currentState.remaining() == 0 || allowed.get() > 0)) {
            Row row = currentState.one();
            if (row == null) {
                AsyncResultSet oldState = currentState;
                currentState = null; // processed this state
                if (oldState.hasMorePages()) {
                    oldState
                            .fetchNextPage()
                            .thenAcceptAsync(this::onResult, executor)
                            .exceptionally(this::onError);
                    return;
                } else {
                    sendOperationComplete();
                    return;
                }
            }
            sendRow(row);
            allowed.decrementAndGet();
        }
    }

    private Void onResult(AsyncResultSet result) {
        currentState = result;
        drain();
        return null;
    }

    public ProducerImpl(CompletionStage<AsyncResultSet> stage) {
        this.stage = stage;
    }

    void sendOperationAborted(@NonNull Throwable error) {
        consumer.operationAborted(error);
    }

    void sendOperationComplete() {
        consumer.operationComplete();
    }

    private void sendRow(@NonNull Row row) {
        consumer.consume(row);
    }

    @Override
    public void register(@NonNull Consumer<? super Row> consumer) {
        if (this.consumer != null) {
            throw new IllegalStateException("Only one consumer is allowed to be registered.");
        }
        this.consumer = consumer;
        stage.thenAcceptAsync(this::onResult, executor).exceptionally(this::onError);
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
        executor.execute(this::drain);
    }

    @Override
    public void cancel() {
        if (this.consumer == null) {
            throw new IllegalStateException("A consumer should be registered first.");
        }
        cancelFlag = true;
    }

    @Override
    Executor getExecutor() {
        return executor;
    }
}
