package br.unb.oss.driver.api.producer;

import java.util.concurrent.Executor;
import java.util.function.Function;

class ProducerMapperImpl<T, U, V> extends ProducerBase<U> implements Consumer<T>, Producer<U> {

    private final Function<T, U> mapper;
    private Consumer<? super U> consumer = null;
    private final ProducerBase<T> parentProducer;

    public ProducerMapperImpl(ProducerBase<T> producer, Function<T, U> mapper) {
        this.parentProducer = producer;
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
        parentProducer.register(this); // start execution
    }

    @Override
    public void produce(long n) {
        parentProducer.produce(n);
    }

    @Override
    public void cancel() {
        parentProducer.cancel();
    }

    @Override
    Executor getExecutor() {
        return parentProducer.getExecutor();
    }
}
