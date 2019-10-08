/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.unb.oss.driver.api.producer;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.cql.CqlRequestAsyncProcessor;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.session.RequestProcessor;

public class CustomRequestProcessor implements RequestProcessor<Request, Producer<Row>> {

  private final CqlRequestAsyncProcessor subProcessor;
  private static final GenericType<?> dummyType = GenericType.of(ProducerImpl.class);

  public static GenericType<?> getResultType() {
    return dummyType;
  }

  CustomRequestProcessor(CqlRequestAsyncProcessor subProcessor) {
    this.subProcessor = subProcessor;
  }

  @Override
  public boolean canProcess(Request request, GenericType<?> resultType) {
    return request instanceof Statement && resultType.equals(dummyType);
  }

  @Override
  public Producer<Row> process(
      Request request,
      DefaultSession session,
      InternalDriverContext context,
      String sessionLogPrefix) {
    Statement statement = (Statement) request;
    ProducerImpl producer =
        new ProducerImpl(subProcessor.process(statement, session, context, sessionLogPrefix));
    return producer;
  }

  @Override
  public Producer<Row> newFailure(RuntimeException error) {
    throw error;
  }
}
