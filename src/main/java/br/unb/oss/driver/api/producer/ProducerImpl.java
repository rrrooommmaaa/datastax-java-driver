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
    private final BlockingQueue<Object> queue = new LinkedBlockingQueue<>();
    private final Semaphore semaphore = new Semaphore(0);

    private Void onError(Throwable error) {

        sendOperationAborted(error);
        return null;
    }

    private Void onResult(AsyncResultSet result) {
        //       try {
        for (;;) {
            Row row = result.one();
            if (row == null) {
                if (result.hasMorePages()) {
                    result.fetchNextPage().thenAccept(this::onResult).exceptionally(this::onError);
                    break;
                } else {
                    sendOperationComplete();
                    break;
                }
            }
            sendRow(row);
        }
        /*       } catch (InterruptedException | ExecutionException ex) {
           Logger.getLogger(ProducerImpl.class.getName()).log(Level.SEVERE, null, ex);
           sendOperationAborted(ex);
       }
         */
        return null;
    }

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

    private void sendRow(@NonNull Row row) {
        try {
            semaphore.acquire(); // TODO:
        } catch (InterruptedException ex) { // TODO:
            Logger.getLogger(ProducerImpl.class.getName()).log(Level.SEVERE, null, ex);
        }
        consumer.consume(row);
        //queue.add(row);
    }

    @Override
    public void register(Consumer<? super Row> consumer) {
        if (this.consumer != null) {
            throw new IllegalStateException("Only one consumer is allowed to be registered.");
        }
        this.consumer = consumer; // TODO: null?
        stage.thenAccept(this::onResult).exceptionally(this::onError);
    }

    @Override
    public void produce(long n) {
        semaphore.release((int) n); // TODO: ???
    }

    @Override
    public void cancel() {
        throw new UnsupportedOperationException(
                "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
    }
}
