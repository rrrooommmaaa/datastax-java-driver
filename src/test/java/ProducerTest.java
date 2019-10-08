/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import br.unb.oss.driver.api.producer.*;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;
import static org.assertj.core.api.Assertions.*;
import org.junit.Before;
import org.mockito.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import org.mockito.internal.stubbing.answers.ReturnsElementsOf;

public class ProducerTest {

    public ProducerTest() {
    }

    @Test
    public void should_consume_requested_number_of_rows_from_unlimited_supply() {
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

        Mockito.verify(consumer, Mockito.after(2000).times(10))
                .consume(Mockito.any());

    }
    
    @Test
    public void should_consume_rows_from_limited_supply_and_operation_complete() {
       
        List<Row> page = IntStream.range(0, 10).mapToObj(x -> Mockito.mock(Row.class)).collect(Collectors.toList());
        page.add(null); // end of page
        
        Consumer<Row> consumer = (Consumer<Row>) Mockito.mock(Consumer.class);
        ArgumentCaptor<Row> captor = ArgumentCaptor.forClass(Row.class);
        doNothing().when(consumer).consume(captor.capture());

        AsyncResultSet asyncResultSet = Mockito.mock(AsyncResultSet.class);
        Mockito.when(asyncResultSet.one()).thenAnswer(new ReturnsElementsOf(page));
        Mockito.when(asyncResultSet.hasMorePages()).thenReturn(false);

        CompletionStage<AsyncResultSet> stage = CompletableFuture.supplyAsync(() -> asyncResultSet);
        Producer<Row> producer = new ProducerImpl(stage);
        producer.register(consumer);
        producer.produce(20);

        Mockito.verify(consumer, Mockito.after(2000).times(10))
                .consume(Mockito.any());

        Mockito.verify(consumer, Mockito.times(1))
                .operationComplete();

    }
    
}
