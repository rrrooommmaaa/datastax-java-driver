package br.unb.oss.driver.api.producer;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A consumer for items produced by a {@link Producer}.
 *
 * @param <T> The type of items this consumer can consume.
 * @see Producer
 */
public interface Consumer<T> {

  /**
   * Invoked by the producer when a new item is available for consumption.
   *
   * <p>The producer MUST signal new items in a serialized fashion (that is, this method should
   * never be invoked concurrently by two or more threads).
   *
   * @param item The item to consume.
   */
  void consume(@NonNull T item);

  /**
   * Invoked by the producer when the operation is complete.
   *
   * <p>Once this method is called, the producer MUST NOT produce any more items.
   *
   * <p>Either this method or {@link #operationAborted(Throwable)} should be called by the producer
   * to signal the end of the operation, but not both.
   *
   * <p>Note that if the consumer {@linkplain Producer#cancel() cancels} the operation, the producer
   * MAY invoke this method or {@link #operationAborted(Throwable)}, but this is not a requirement.
   */
  void operationComplete();

  /**
   * Invoked by the producer when the operation was aborted due to an error.
   *
   * <p>Once this method is called, the producer MUST NOT produce any more items.
   *
   * <p>Either this method or {@link #operationComplete()} should be called by the producer to
   * signal the end of the operation, but not both.
   *
   * <p>Note that if the consumer {@linkplain Producer#cancel() cancels} the operation, the producer
   * MAY invoke this method or {@link #operationComplete()}, but this is not a requirement.
   *
   * @param error The error that caused the operation to abort.
   */
  void operationAborted(@NonNull Throwable error);
}
