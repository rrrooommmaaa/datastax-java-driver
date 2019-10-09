package br.unb.oss.driver.api.producer;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ProducerImpl<U> extends ProducerBase<Row, U> implements Producer<Row> {

    private final CompletionStage<AsyncResultSet> stage;
    private volatile Consumer consumer = null;
    private final BlockingQueue<Long> produceRequests = new LinkedBlockingQueue<>();
    private long allowed = 0;

    private Void onError(Throwable error) {
        sendOperationAborted(error.getCause());
        return null;
    }

    private static Executor executor = Executors.newCachedThreadPool();
    
    private Void onResult(AsyncResultSet result) {
        for (;;) {
            Row row = result.one();
            if (row == null) {
                if (result.hasMorePages()) {
                    result.fetchNextPage().thenAcceptAsync(this::onResult, executor).exceptionally(this::onError);
                    break;
                } else {
                    sendOperationComplete();
                    break;
                }
            }
            if (!sendRow(row)) {
                // Cancelled
                break;
            }
        }
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

    private boolean sendRow(@NonNull Row row) {
        if (allowed <= 0) {
            try {
                Long newAllowed = produceRequests.take();
                if (newAllowed == 0) {
                    return false; // cancel signalled
                }
                allowed = newAllowed;
            } catch (InterruptedException ex) {
                Logger.getLogger(ProducerImpl.class.getName()).log(Level.SEVERE, null, ex);
                return false;
            }
        }
        allowed--;
        consumer.consume(row);
        return true;
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
        produceRequests.add(n);
    }

    @Override
    public void cancel() {
        if (this.consumer == null) {
            throw new IllegalStateException("A consumer should be registered first.");
        }
        produceRequests.add(0L);
    }
}
