import br.unb.oss.driver.api.producer.Producer;
import br.unb.oss.driver.api.producer.ProducerConsumerSession;
import br.unb.oss.driver.api.producer.ProducerConsumerSessionBuilder;
import br.unb.oss.driver.api.producer.TestConsumer;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class ProducerConsumerIT2 {

  private static MyCcmRule ccmRule = MyCcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccmRule).build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  @BeforeClass
  public static void createTable() {
    CqlSession session = sessionRule.session();
    session.execute("CREATE TABLE t1 (pk int, cc int, v int, PRIMARY KEY (pk, cc))");
  }

  @Test
  public void should_produce_all_rows_single_page() throws InterruptedException {
    insertRows(10);
    try (ProducerConsumerSession session = newProducerConsumerSession()) {
      Producer<Row> producer = session.produce("SELECT v FROM t1 WHERE pk = 0");
      TestConsumer consumer = new TestConsumer();
      producer.register(consumer);
      producer.produce(10);
      consumer.awaitTermination();
      assertThat(consumer.getRows()).hasSize(10);
      assertThat(consumer.isTerminated()).isTrue();
      assertThat(consumer.getError()).isNull();
    }
  }

  @Test
  public void should_abort() throws InterruptedException {
    insertRows(10);
    try (ProducerConsumerSession session = newProducerConsumerSession()) {
      Producer<Row> producer = session.produce("SELECT nonexistent FROM t1 WHERE pk = 0");
      TestConsumer consumer = new TestConsumer();
      producer.register(consumer);
      consumer.awaitTermination();
      assertThat(consumer.getRows()).hasSize(0);
      assertThat(consumer.isTerminated()).isTrue();
      assertThat(consumer.getError()).isInstanceOf(InvalidQueryException.class);
    }
  }

  @Test
  public void should_produce_all_rows_multi_page() throws InterruptedException {
    insertRows(40);
    try (ProducerConsumerSession session = newProducerConsumerSession()) {
      Producer<Row> producer = session.produce("SELECT v FROM t1 WHERE pk = 0");
      TestConsumer consumer = new TestConsumer();
      producer.register(consumer);
      // exercise page boundaries
      producer.produce(10);
      producer.produce(9);
      producer.produce(11);
      producer.produce(9);
      producer.produce(2); // requested 41 total
      consumer.awaitTermination();
      assertThat(consumer.getRows()).hasSize(40);
      assertThat(consumer.isTerminated()).isTrue();
      assertThat(consumer.getError()).isNull();
    }
  }

  @Test
  public void should_produce_partial_rows() throws InterruptedException {
    insertRows(100);
    try (ProducerConsumerSession session = newProducerConsumerSession()) {
      Producer<Row> producer = session.produce("SELECT v FROM t1 WHERE pk = 0");
      TestConsumer consumer = new TestConsumer();
      producer.register(consumer);
      producer.produce(50);
      consumer.awaitProduction(50);
      producer.cancel();
      producer.produce(50); // should be no-op
      assertThat(consumer.getRows()).hasSize(50);
      assertThat(consumer.isTerminated()).isFalse();
      assertThat(consumer.getError()).isNull();
    }
  }

  private ProducerConsumerSession newProducerConsumerSession() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, 10)
            .build();
    return new ProducerConsumerSessionBuilder()
        .withKeyspace(sessionRule.keyspace())
        .withConfigLoader(loader)
        .build();
  }

  private void insertRows(int rows) {
    CqlSession session = sessionRule.session();
    session.execute("TRUNCATE t1");
    for (int i = 0; i < rows; i++) {
      session.execute(
          SimpleStatement.newInstance("INSERT INTO t1 (pk, cc, v) VALUES (0, ?, ?)", i, i));
    }
  }
}
