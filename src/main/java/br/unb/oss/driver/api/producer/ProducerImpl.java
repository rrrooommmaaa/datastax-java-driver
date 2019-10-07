/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.unb.oss.driver.api.producer;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ProducerImpl implements Producer<Row> {

  private final CompletionStage<AsyncResultSet> stage;
  private AsyncResultSet result;

  ProducerImpl(CompletionStage<AsyncResultSet> stage) {
    this.stage = stage;
  }

  void sendOperationAborted(@NonNull Throwable error) {}

  void sendOperationComplete() {}

  void sendRow(@NonNull Row row) {}

  @Override
  public void register(Consumer<? super Row> consumer) {
    throw new UnsupportedOperationException(
        "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public void produce(long n) {
    // TODO: checks
    try {
      if (this.result == null) {
        this.result = stage.toCompletableFuture().get(); // TODO: async
      }

      while (n-- > 0) {
        Row row = null;
        while (row == null) {
          row = this.result.one();
          if (row == null) {
            if (this.result.hasMorePages()) {
              this.result = this.result.fetchNextPage().toCompletableFuture().get(); // TODO: async
            } else {
              break;
            }
          }
        }
        if (row == null) {
          sendOperationComplete(); // TODO: duplicate?
          break;
        } else {
          sendRow(row);
        }
      }
    } catch (InterruptedException | ExecutionException ex) {
      Logger.getLogger(ProducerImpl.class.getName()).log(Level.SEVERE, null, ex);
      sendOperationAborted(ex);
    }
  }

  @Override
  public void cancel() {
    throw new UnsupportedOperationException(
        "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
  }
}
