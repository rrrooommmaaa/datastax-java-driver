package br.unb.oss.driver.api.producer;

import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
// import com.datastax.oss.driver.api.core.cql.PrepareRequest;
// import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.cql.CqlPrepareAsyncProcessor;
// import com.datastax.oss.driver.internal.core.cql.CqlPrepareSyncProcessor;
import com.datastax.oss.driver.internal.core.cql.CqlRequestAsyncProcessor;
import com.datastax.oss.driver.internal.core.cql.CqlRequestSyncProcessor;
import com.datastax.oss.driver.internal.core.session.RequestProcessorRegistry;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class ProducerConsumerDriverContext extends DefaultDriverContext {

  public ProducerConsumerDriverContext(
      DriverConfigLoader configLoader,
      List<TypeCodec<?>> typeCodecs,
      NodeStateListener nodeStateListener,
      SchemaChangeListener schemaChangeListener,
      RequestTracker requestTracker,
      Map<String, String> localDatacenters,
      Map<String, Predicate<Node>> nodeFilters,
      ClassLoader classLoader) {
    super(
        configLoader,
        typeCodecs,
        nodeStateListener,
        schemaChangeListener,
        requestTracker,
        localDatacenters,
        nodeFilters,
        classLoader);
  }

  @Override
  public RequestProcessorRegistry buildRequestProcessorRegistry() {
    // Register the typical request processors, except instead of the normal async processors,
    // use GuavaRequestAsyncProcessor to return ListenableFutures in async methods.

    CqlRequestAsyncProcessor cqlRequestAsyncProcessor = new CqlRequestAsyncProcessor();
    CqlPrepareAsyncProcessor cqlPrepareAsyncProcessor = new CqlPrepareAsyncProcessor();
    CqlRequestSyncProcessor cqlRequestSyncProcessor =
        new CqlRequestSyncProcessor(cqlRequestAsyncProcessor);

    return new RequestProcessorRegistry(
        getSessionName(),

        cqlRequestAsyncProcessor,
//        new CqlPrepareSyncProcessor(cqlPrepareAsyncProcessor),
            
            
//        new GuavaRequestAsyncProcessor<>(
//            cqlRequestAsyncProcessor, Statement.class, GuavaSession.ASYNC),
//        new GuavaRequestAsyncProcessor<>(
//            cqlPrepareAsyncProcessor, PrepareRequest.class, GuavaSession.ASYNC_PREPARED),
        new CustomRequestProcessor(cqlRequestAsyncProcessor));
  }
}
