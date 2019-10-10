
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import br.unb.oss.driver.api.producer.Producer;
import br.unb.oss.driver.api.producer.ProducerImpl;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
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
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TestBase {

    @Mock
    protected ColumnDefinitions columnDefinitions;
    @Mock
    protected CqlSession session;
    @Mock
    protected InternalDriverContext context;
    @Mock
    protected Statement statement;

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

    protected ExecutionInfo mockExecutionInfo() {
        ExecutionInfo executionInfo = mock(ExecutionInfo.class);
        when(executionInfo.getStatement()).thenAnswer(invocation -> statement);
        return executionInfo;
    }

    protected Queue<List<ByteBuffer>> mockData(int start, int end) {
        Queue<List<ByteBuffer>> data = new ArrayDeque<>();
        for (int i = start; i < end; i++) {
            data.add(Lists.newArrayList(TypeCodecs.INT.encode(i, DefaultProtocolVersion.DEFAULT)));
        }
        return data;
    }

    static final int PAGE_SIZE = 10;

    protected CompletionStage<AsyncResultSet> generateChainPage() {
        ExecutionInfo executionInfo1 = mockExecutionInfo();
        DefaultAsyncResultSet resultSet1
                = new DefaultAsyncResultSet(
                        columnDefinitions, executionInfo1, mockData(0, PAGE_SIZE), session, context);

        ByteBuffer mockPagingState = ByteBuffer.allocate(0);
        when(executionInfo1.getPagingState()).thenReturn(mockPagingState);
        Statement<?> mockNextStatement = mock(Statement.class);
        when(((Statement) statement).copy(mockPagingState)).thenReturn(mockNextStatement);
        when(session.executeAsync(mockNextStatement)).thenAnswer(invocation -> generateChainPage());

        return CompletableFuture.supplyAsync(() -> resultSet1);
    }

    protected Producer<Row> generateLimitedProducer(int size) {
        ExecutionInfo executionInfo1 = mockExecutionInfo();
        DefaultAsyncResultSet resultSet1
                = new DefaultAsyncResultSet(
                        columnDefinitions, executionInfo1, mockData(0, size), session, context);

        CompletionStage<AsyncResultSet> stage = CompletableFuture.supplyAsync(() -> resultSet1);
        Producer<Row> producer = new ProducerImpl(stage);
        return producer;
    }
}
