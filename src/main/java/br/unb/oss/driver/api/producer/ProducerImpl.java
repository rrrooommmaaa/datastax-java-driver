/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.unb.oss.driver.api.producer;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ProducerImpl implements Producer<Row> {

    private final CompletionStage<AsyncResultSet> stage;
    private Consumer consumer = null;
    private final BlockingQueue<Long> queue = new LinkedBlockingQueue<>();
    private long allowed = 0;
    // private final Semaphore semaphore = new Semaphore(0);

    private Void onError(Throwable error) {

        sendOperationAborted(error.getCause());
        return null;
    }

    private Void onResult(AsyncResultSet result) {
        //       try {
        for (;;) {
            Row row = result.one();
            if (row == null) {
                if (result.hasMorePages()) {
                    result.fetchNextPage().thenAcceptAsync(this::onResult).exceptionally(this::onError);
                    break;
                } else {
                    sendOperationComplete();
                    break;
                }
            }
            if (!sendRow(row)) {
                // TODO: what to do on cancel
                break;
            }
        }
        /*       } catch (InterruptedException | ExecutionException ex) {
           Logger.getLogger(ProducerImpl.class.getName()).log(Level.SEVERE, null, ex);
           sendOperationAborted(ex);
       }
         */
        return null;
    }

    /*
    private void deque() {
        for (;;) {
            try {
                Object obj = queue.take();
                if (obj instanceof Row) {
                    semaphore.acquire();
                    consumer.consume(obj);
                } else if (obj instanceof Throwable) {
                    consumer.operationAborted((Throwable) obj);
                    break;
                } else {
                    consumer.operationComplete();
                    break;
                }
            } catch (InterruptedException ex) {
                Logger.getLogger(ProducerImpl.class.getName()).log(Level.SEVERE, null, ex);
                // TODO: ????
            }
        }
    }
     */
    ProducerImpl(CompletionStage<AsyncResultSet> stage) {
        this.stage = stage;
    }

    void sendOperationAborted(@NonNull Throwable error) {
        consumer.operationAborted(error);
        // queue.add(error);
    }

    void sendOperationComplete() {
        consumer.operationComplete();
        // queue.add(null);
    }

    private boolean sendRow(@NonNull Row row) {
        if (allowed <= 0) {
            try {
                Long newAllowed = queue.take();
                if (newAllowed == 0) {
                    return false;
                }
                allowed = newAllowed;
            } catch (InterruptedException ex) { // TODO:
                Logger.getLogger(ProducerImpl.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        allowed--;
        consumer.consume(row);
        return true;
    }

    @Override
    public void register(Consumer<? super Row> consumer) {
        if (this.consumer != null) {
            throw new IllegalStateException("Only one consumer is allowed to be registered.");
        }
        this.consumer = consumer; // TODO: null?
        stage.thenAcceptAsync(this::onResult).exceptionally(this::onError);
    }

    @Override
    public void produce(long n) {
        if (n <= 0) {
            throw new IllegalArgumentException("You should request more than zero rows.");
        } // TODO: checks
        queue.add(n); // TODO: ???
    }

    @Override
    public void cancel() {
        queue.add(0L);
    }
}
