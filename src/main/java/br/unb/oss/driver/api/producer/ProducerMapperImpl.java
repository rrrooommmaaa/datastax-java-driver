package br.unb.oss.driver.api.producer;

import java.util.function.Function;

public class ProducerMapperImpl<T,U,V> extends ProducerBase<U,V> implements Consumer<T>, Producer<U> {
    private final Function<T,U> mapper;
    private Consumer<? super U> consumer = null;
    private final Producer<T> producer;

    public ProducerMapperImpl(Producer<T> producer, Function<T,U> mapper) {
        this.producer = producer;
        this.mapper = mapper;
    }
    
    @Override
    public void consume(T item) {
        consumer.consume(mapper.apply(item));
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
    public void register(Consumer<? super U> consumer) {
        if (this.consumer != null) {
            throw new IllegalStateException("Only one consumer is allowed to be registered.");
        }
        this.consumer = consumer;
        producer.register(this); // start execution
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
