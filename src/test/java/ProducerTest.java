import br.unb.oss.driver.api.producer.*;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;
import org.mockito.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import org.mockito.internal.stubbing.answers.ReturnsElementsOf;

public class ProducerTest {   
    @Test
    public void should_consume_requested_number_of_rows_from_unlimited_supply() {
        Consumer<Row> consumer = (Consumer<Row>) mock(Consumer.class);

        AsyncResultSet asyncResultSet = mock(AsyncResultSet.class);
        when(asyncResultSet.one()).thenReturn(mock(Row.class));
        when(asyncResultSet.hasMorePages()).thenReturn(false);

        CompletionStage<AsyncResultSet> stage = CompletableFuture.supplyAsync(() -> asyncResultSet);
        Producer<Row> producer = new ProducerImpl(stage);
        producer.register(consumer);
        producer.produce(10);

        // verify that consumer.consume was called exactly 10 times
        verify(consumer, after(2000).times(10))
                .consume(any());

    }

    private List<Row> create_page(int size) {
        List<Row> page = IntStream.range(0, size).mapToObj(x -> mock(Row.class)).collect(Collectors.toList());
        page.add(null); // end of page
        return page;
    }

    private Producer<Row> generate_limited_producer(int size) {
        AsyncResultSet asyncResultSet = mock(AsyncResultSet.class);
        when(asyncResultSet.one()).thenAnswer(new ReturnsElementsOf(create_page(size)));
        when(asyncResultSet.hasMorePages()).thenReturn(false);

        CompletionStage<AsyncResultSet> stage = CompletableFuture.supplyAsync(() -> asyncResultSet);
        Producer<Row> producer = new ProducerImpl(stage);
        return producer;
    }

    @Test
    public void should_consume_rows_from_limited_supply_and_operation_complete() {
        Consumer<Row> consumer = (Consumer<Row>) mock(Consumer.class);

        Producer<Row> producer = generate_limited_producer(10);
        producer.register(consumer);
        producer.produce(20);

        // verify that consumer.operationComplete was called 1 time
        verify(consumer, after(6000).times(1))
                .operationComplete();

        // verify that consumer.consume was called exactly 10 times
        verify(consumer, times(10))
                .consume(any());

    }

    final static int PAGE_SIZE = 10;

    private CompletionStage<AsyncResultSet> generateChainPage() {
        AsyncResultSet asyncResultSet = mock(AsyncResultSet.class);
        when(asyncResultSet.one()).thenAnswer(new ReturnsElementsOf(create_page(PAGE_SIZE)));
        when(asyncResultSet.hasMorePages()).thenReturn(true);
        when(asyncResultSet.fetchNextPage()).thenAnswer(invocation -> generateChainPage());
        return CompletableFuture.supplyAsync(() -> asyncResultSet);
    }

    @Test
    public void should_consume_requested_number_of_rows_from_paged_supply() {
        Consumer<Row> consumer = (Consumer<Row>) mock(Consumer.class);
        doNothing().when(consumer).consume(any());
        
        CompletionStage<AsyncResultSet> stage = generateChainPage();
        Producer<Row> producer = new ProducerImpl(stage);
        producer.register(consumer);
        producer.produce(5);
        producer.produce(PAGE_SIZE);
        producer.produce(PAGE_SIZE);

        // verify that consumer.consume was called exactly this number of times
        verify(consumer, after(6000).times(PAGE_SIZE * 2 + 5))
                .consume(any());

    }

    @Test
    public void should_receive_aborted_on_exception() {
        Consumer<Row> consumer = (Consumer<Row>) mock(Consumer.class);
        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        doNothing().when(consumer).operationAborted(captor.capture());

        CompletionStage<AsyncResultSet> stage = CompletableFuture.supplyAsync(() -> {
            throw new InvalidQueryException(null, "test");
        });
        Producer<Row> producer = new ProducerImpl(stage);
        producer.register(consumer);
        producer.produce(10);

        verify(consumer, after(2000).times(0))
                .consume(any());
        verify(consumer, times(1))
                .operationAborted(any(InvalidQueryException.class));

    }

    @Test(expected = IllegalStateException.class)
    public void should_throw_when_registering_two_consumers() {
        Consumer<Row> consumer1 = (Consumer<Row>) mock(Consumer.class);
        Consumer<Row> consumer2 = (Consumer<Row>) mock(Consumer.class);

        Producer<Row> producer = generate_limited_producer(10);
        producer.register(consumer1);
        producer.register(consumer2);

        producer.produce(10);
    }

    @Test(expected = IllegalStateException.class)
    public void should_throw_when_requesting_before_registration() {
        Producer<Row> producer = generate_limited_producer(10);

        producer.produce(10);
    }

    @Test(expected = IllegalArgumentException.class)
    public void should_throw_when_requesting_negative_qty() {
        Consumer<Row> consumer = (Consumer<Row>) mock(Consumer.class);

        Producer<Row> producer = generate_limited_producer(10);
        producer.register(consumer);

        producer.produce(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void should_throw_when_requesting_zero_qty() {
        Consumer<Row> consumer = (Consumer<Row>) mock(Consumer.class);

        Producer<Row> producer = generate_limited_producer(10);
        producer.register(consumer);

        producer.produce(0);
    }

}
