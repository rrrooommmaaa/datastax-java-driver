package br.unb.oss.driver.api.producer;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A Producer of items in a typical producer-consumer pattern.
 *
 * @param <T> The type of items produced by this producer.
 * @see Consumer
 */
public interface Producer<T> {

  /**
   * Registers the given consumer against this producer.
   *
   * <p>This method MUST be invoked before other methods of this produced are called.
   *
   * <p>This method MUST be called exactly once; registering more than one consumer MUST result in
   * an error being thrown.
   *
   * @param consumer The consumer to register.
   * @throws IllegalStateException If this producer was already registered with a different
   *     consumer.
   */
  void register(@NonNull Consumer<? super T> consumer);

  /**
   * Requests the producer to produce at most {@code n} items.
   *
   * <p>This method MUST NOT be called before {@link #register(Consumer)}; failing to do so MUST
   * result in an error being thrown.
   *
   * <p>The producer can produce fewer items than requested; it MUST NOT produce more items than
   * requested.
   *
   * @param n The maximum number of items to produce.
   * @throws IllegalStateException If this producer is not yet registered with a consumer.
   * @throws IllegalArgumentException If {@code n} is equal to or lesser than zero.
   */
  void produce(long n);

  /**
   * Requests the producer to cancel the operation.
   *
   * <p>This method MUST NOT be called before {@link #register(Consumer)}; failing to do so MUST
   * result in an error being thrown.
   *
   * <p>Once this method is invoked, the producer MUST eventually stop producing items.
   *
   * @throws IllegalStateException If this producer is not yet registered with a consumer.
   */
  void cancel();

  <U> Producer<U> map(Function<T,U> mapper);

  Producer<T> reduce(BiFunction<T,T,T> reducer);

  Producer<T> filter(Predicate<T> filter);

}
