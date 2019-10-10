import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import br.unb.oss.driver.api.producer.Consumer;
import br.unb.oss.driver.api.producer.Producer;
import com.datastax.oss.driver.api.core.cql.Row;
import java.util.function.BiFunction;
import org.junit.Test;

public class ProducerReducerTest extends TestBase {
  @Test
  public void should_consume_one_row_and_operation_complete() {
    Consumer<Row> consumer = (Consumer<Row>) mock(Consumer.class);

    Producer<Row> parentProducer = generateLimitedProducer(10);
    BiFunction<Row, Row, Row> reducer = (x, y) -> y;
    Producer<Row> producer = parentProducer.reduce(reducer);

    producer.register(consumer);
    producer.produce(1);

    // verify that consumer.consume was called exactly 1 times
    verify(consumer, after(2000).times(1)).consume(any());

    // verify that consumer.operationComplete was called exactly 1 times
    verify(consumer, times(1)).operationComplete();
  }
}
