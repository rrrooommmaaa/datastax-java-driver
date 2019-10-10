
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import br.unb.oss.driver.api.producer.Consumer;
import br.unb.oss.driver.api.producer.Producer;
import br.unb.oss.driver.api.producer.ProducerImpl;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import org.junit.Test;

public class ProducerFilterTest extends TestBase {

    @Test
    public void should_consume_requested_number_of_rows_from_unlimited_supply() {
        Consumer<Row> consumer = (Consumer<Row>) mock(Consumer.class);

        CompletionStage<AsyncResultSet> stage = generateChainPage();
        Producer<Row> parentProducer = new ProducerImpl(stage);
        Predicate<Row> filter = x -> x.get("ID", Integer.class) == 0;
        Producer<Row> producer = parentProducer.filter(filter);

        producer.register(consumer);
        producer.produce(10);

        // verify that consumer.consume was called exactly 10 times
        verify(consumer, after(2000).times(10)).consume(any());
    }

    @Test
    public void should_consume_one_matching_record_and_operation_complete() {
        Consumer<Row> consumer = (Consumer<Row>) mock(Consumer.class);

        Producer<Row> parentProducer = generateLimitedProducer(10);
        Predicate<Row> filter = x -> x.get("ID", Integer.class) == 0;
        Producer<Row> producer = parentProducer.filter(filter);

        producer.register(consumer);
        producer.produce(1);

        // verify that consumer.consume was called exactly 1 time
        verify(consumer, after(2000).times(1)).consume(any());

        // verify that consumer.operationComplete was called exactly 1 time
        verify(consumer, times(1)).operationComplete();
    }
}
