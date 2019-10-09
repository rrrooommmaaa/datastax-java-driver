import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import br.unb.oss.driver.api.producer.*;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.cql.DefaultAsyncResultSet;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

public class ProducerTest {

  @Mock private ColumnDefinitions columnDefinitions;
  @Mock private CqlSession session;
  @Mock private InternalDriverContext context;
  @Mock private Statement statement;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    // One single column "i" of type int:
    when(columnDefinitions.contains("i")).thenReturn(true);
    ColumnDefinition iDefinition = mock(ColumnDefinition.class);
    when(iDefinition.getType()).thenReturn(DataTypes.INT);
    when(columnDefinitions.get("i")).thenReturn(iDefinition);
    when(columnDefinitions.firstIndexOf("i")).thenReturn(0);
    when(columnDefinitions.get(0)).thenReturn(iDefinition);

    when(context.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT);
    when(context.getProtocolVersion()).thenReturn(DefaultProtocolVersion.DEFAULT);
  }

  private ExecutionInfo mockExecutionInfo() {
    ExecutionInfo executionInfo = mock(ExecutionInfo.class);
    when(executionInfo.getStatement()).thenAnswer(invocation -> statement);
    return executionInfo;
  }

  private Queue<List<ByteBuffer>> mockData(int start, int end) {
    Queue<List<ByteBuffer>> data = new ArrayDeque<>();
    for (int i = start; i < end; i++) {
      data.add(Lists.newArrayList(TypeCodecs.INT.encode(i, DefaultProtocolVersion.DEFAULT)));
    }
    return data;
  }

  @Test
  public void should_consume_requested_number_of_rows_from_unlimited_supply() {
    Consumer<Row> consumer = (Consumer<Row>) mock(Consumer.class);

    int portion = PAGE_SIZE / 2;

    CompletionStage<AsyncResultSet> stage = generateChainPage();
    Producer<Row> producer = new ProducerImpl(stage);
    producer.register(consumer);
    producer.produce(portion);

    // verify that consumer.consume was called exactly 'portion' times
    verify(consumer, after(2000).times(portion)).consume(any());
  }

  private List<Row> create_page(int size) {
    List<Row> page =
        IntStream.range(0, size).mapToObj(x -> mock(Row.class)).collect(Collectors.toList());
    page.add(null); // end of page
    return page;
  }

  private Producer<Row> generate_limited_producer(int size) {
    ExecutionInfo executionInfo1 = mockExecutionInfo();
    DefaultAsyncResultSet resultSet1 =
        new DefaultAsyncResultSet(
            columnDefinitions, executionInfo1, mockData(0, size), session, context);

    CompletionStage<AsyncResultSet> stage = CompletableFuture.supplyAsync(() -> resultSet1);
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
    verify(consumer, after(6000).times(1)).operationComplete();

    // verify that consumer.consume was called exactly 10 times
    verify(consumer, times(10)).consume(any());
  }

  static final int PAGE_SIZE = 10;

  private CompletionStage<AsyncResultSet> generateChainPage() {
    ExecutionInfo executionInfo1 = mockExecutionInfo();
    DefaultAsyncResultSet resultSet1 =
        new DefaultAsyncResultSet(
            columnDefinitions, executionInfo1, mockData(0, PAGE_SIZE), session, context);

    ByteBuffer mockPagingState = ByteBuffer.allocate(0);
    when(executionInfo1.getPagingState()).thenReturn(mockPagingState);
    Statement<?> mockNextStatement = mock(Statement.class);
    when(((Statement) statement).copy(mockPagingState)).thenReturn(mockNextStatement);
    when(session.executeAsync(mockNextStatement)).thenAnswer(invocation -> generateChainPage());

    return CompletableFuture.supplyAsync(() -> resultSet1);
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
    verify(consumer, after(6000).times(PAGE_SIZE * 2 + 5)).consume(any());
  }

  @Test
  public void should_receive_aborted_on_exception() {
    Consumer<Row> consumer = (Consumer<Row>) mock(Consumer.class);
    ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
    doNothing().when(consumer).operationAborted(captor.capture());

    CompletionStage<AsyncResultSet> stage =
        CompletableFuture.supplyAsync(
            () -> {
              throw new InvalidQueryException(null, "test");
            });
    Producer<Row> producer = new ProducerImpl(stage);
    producer.register(consumer);
    producer.produce(10);

    verify(consumer, after(2000).times(0)).consume(any());
    verify(consumer, times(1)).operationAborted(any(InvalidQueryException.class));
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
