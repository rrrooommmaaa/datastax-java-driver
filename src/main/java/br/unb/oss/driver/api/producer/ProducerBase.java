package br.unb.oss.driver.api.producer;

import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

public abstract class ProducerBase<T> implements Producer<T> {
  @Override
  public <U> Producer<U> map(Function<T, U> mapper) {
    return new ProducerMapperImpl(this, mapper);
  }

  @Override
  public Producer<T> reduce(BiFunction<T, T, T> reducer) {
    return new ProducerReducerImpl(this, reducer);
  }

  @Override
  public Producer<T> filter(Predicate<T> filter) {
    return new ProducerFilterImpl(this, filter);
  }

  abstract Executor getExecutor();
}
