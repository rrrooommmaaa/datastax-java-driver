
import br.unb.oss.driver.api.producer.Consumer;
import br.unb.oss.driver.api.producer.Producer;
import br.unb.oss.driver.api.producer.ProducerImpl;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.junit.Test;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProducerMapperTest {
    @Test
    public void should_consume_requested_number_of_rows_from_unlimited_supply() {
        Consumer<Integer> consumer = (Consumer<Integer>) mock(Consumer.class);

        AsyncResultSet asyncResultSet = mock(AsyncResultSet.class);
        when(asyncResultSet.one()).thenReturn(mock(Row.class));
        when(asyncResultSet.hasMorePages()).thenReturn(false);

        CompletionStage<AsyncResultSet> stage = CompletableFuture.supplyAsync(() -> asyncResultSet);
        Producer<Row> subProducer = new ProducerImpl(stage);
        Function<Row, Integer> mapper = x -> Integer.MIN_VALUE;
        Producer<Integer> producer = subProducer.map(mapper);
    
        producer.register(consumer);
        producer.produce(10);

        // verify that consumer.consume was called exactly 10 times
        verify(consumer, after(2000).times(10))
                .consume(any(Integer.class));

    }
}
