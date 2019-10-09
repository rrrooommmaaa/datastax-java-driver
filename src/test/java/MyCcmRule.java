import com.datastax.oss.driver.categories.ParallelizableTests;
import java.lang.reflect.Method;
import org.junit.AssumptionViolatedException;
import org.junit.experimental.categories.Category;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A rule that creates a globally shared single node Ccm cluster that is only shut down after the
 * JVM exists.
 *
 * <p>Note that this rule should be considered mutually exclusive with {@link CustomCcmRule}.
 * Creating instances of these rules can create resource issues.
 */
public class MyCcmRule extends MyBaseCcmRule {

  private static final MyCcmRule INSTANCE = new MyCcmRule();

  private volatile boolean started = false;

  private MyCcmRule() {
    super(configureCcmBridge(MyCcmBridge.builder()).build());
  }

  public static MyCcmBridge.Builder configureCcmBridge(MyCcmBridge.Builder builder) {
    Logger logger = LoggerFactory.getLogger(MyCcmRule.class);
    String customizerClass =
        System.getProperty(
            "ccmrule.bridgecustomizer",
            "com.datastax.oss.driver.api.testinfra.ccm.DefaultCcmBridgeBuilderCustomizer");
    try {
      Class<?> clazz = Class.forName(customizerClass);
      Method method = clazz.getMethod("configureBuilder", MyCcmBridge.Builder.class);
      return (MyCcmBridge.Builder) method.invoke(null, builder);
    } catch (Exception e) {
      logger.warn(
          "Could not find CcmRule customizer {}, will use the default CcmBridge.",
          customizerClass,
          e);
      return builder;
    }
  }

  @Override
  protected synchronized void before() {
    if (!started) {
      // synchronize before so blocks on other before() call waiting to finish.
      super.before();
      started = true;
    }
  }

  @Override
  protected void after() {
    // override after so we don't remove when done.
  }

  @Override
  public Statement apply(Statement base, Description description) {

    Category categoryAnnotation = description.getTestClass().getAnnotation(Category.class);
    if (categoryAnnotation == null
        || categoryAnnotation.value().length != 1
        || categoryAnnotation.value()[0] != ParallelizableTests.class) {
      return new Statement() {
        @Override
        public void evaluate() {
          throw new AssumptionViolatedException(
              String.format(
                  "Tests using %s must be annotated with `@Category(%s.class)`. Description: %s",
                  MyCcmRule.class.getSimpleName(),
                  ParallelizableTests.class.getSimpleName(),
                  description));
        }
      };
    }

    return super.apply(base, description);
  }

  public void reloadCore(int node, String keyspace, String table, boolean reindex) {
    ccmBridge.reloadCore(node, keyspace, table, reindex);
  }

  public static MyCcmRule getInstance() {
    return INSTANCE;
  }
}
