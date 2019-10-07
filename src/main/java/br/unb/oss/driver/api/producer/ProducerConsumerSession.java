package br.unb.oss.driver.api.producer;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import edu.umd.cs.findbugs.annotations.NonNull;

/** A session that executes CQL requests in a producer-consumer fashion. */
public interface ProducerConsumerSession extends CqlSession {

  /**
   * Returns a producer that, once registered with a consumer, will execute the query and emit all
   * retrieved rows to its consumer.
   *
   * @param query The query to execute.
   */
  @NonNull
  default Producer<Row> produce(@NonNull String query) {
    // TODO implement
    return null;
  }

  /**
   * Returns a producer that, once registered with a consumer, will execute the statement and emit
   * all retrieved rows to its consumer.
   *
   * @param statement The statement to execute.
   */
  @NonNull
  default Producer<Row> produce(@NonNull Statement<?> statement) {
    // TODO implement
    return null;
  }
}
