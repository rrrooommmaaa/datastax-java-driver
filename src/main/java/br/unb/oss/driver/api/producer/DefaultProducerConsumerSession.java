package br.unb.oss.driver.api.producer;

import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.session.SessionWrapper;

public class DefaultProducerConsumerSession extends SessionWrapper
    implements ProducerConsumerSession {

  public DefaultProducerConsumerSession(Session delegate) {
    super(delegate);
  }
}
