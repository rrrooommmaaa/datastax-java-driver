package br.unb.oss.driver.api.producer;

import java.util.function.Predicate;

public class ProducerFilterImpl<T, U> extends ProducerBase<T,U> implements Consumer<T>, Producer<T> {
    private final Predicate<T> filter;
    private Consumer<? super T> consumer = null;
    private final Producer<T> producer;

    public ProducerFilterImpl(Producer<T> producer, Predicate<T> filter) {
        this.producer = producer;
        this.filter =  filter;
    }
    
    @Override
    public void consume(T item) {
        consumer.consume(item);
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
        producer.register(this);
    }

    @Override
    public void produce(long n) {
        producer.produce(n);
    }

    @Override
    public void cancel() {
        producer.cancel();
    }
  
}
