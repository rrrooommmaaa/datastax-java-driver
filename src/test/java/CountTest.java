
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import br.unb.oss.driver.api.producer.Consumer;
import br.unb.oss.driver.api.producer.Producer;
import org.junit.Test;

public class CountTest extends TestBase {

    @Test
    public void should_count_with_map_reduce() {
        Consumer<Integer> consumer = (Consumer<Integer>) mock(Consumer.class);

        final int COUNT = 33;

        Producer<Integer> producer = generateLimitedProducer(COUNT).map(x -> 1).reduce((x, y) -> x + y);

        producer.register(consumer);
        producer.produce(1);

        // verify that consumer.consume was called exactly 1 time with COUNT value
        verify(consumer, after(2000).times(1)).consume(COUNT);

        // verify that consumer.operationComplete was called exactly 1 time
        verify(consumer, times(1)).operationComplete();
    }
}
