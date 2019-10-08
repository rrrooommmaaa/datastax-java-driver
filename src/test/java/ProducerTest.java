/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import br.unb.oss.driver.api.producer.*;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.Test;
import static org.assertj.core.api.Assertions.*;
import org.mockito.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class ProducerTest {

    public ProducerTest() {
    }

    @Test
    public void should_consume_all_rows() throws InterruptedException {
        Consumer<Row> consumer = (Consumer<Row>) Mockito.mock(Consumer.class);
        ArgumentCaptor<Row> captor = ArgumentCaptor.forClass(Row.class);
        doNothing().when(consumer).consume(captor.capture());

        AsyncResultSet asyncResultSet = Mockito.mock(AsyncResultSet.class);
        Mockito.when(asyncResultSet.one()).thenReturn(Mockito.mock(Row.class));
        Mockito.when(asyncResultSet.hasMorePages()).thenReturn(false);

        CompletionStage<AsyncResultSet> stage = CompletableFuture.supplyAsync(() -> asyncResultSet);
        Producer<Row> producer = new ProducerImpl(stage);
        producer.register(consumer);
        producer.produce(10);

        Mockito.verify(consumer, Mockito.after(2000).times(9))
                .consume(Mockito.any());

    }
}
