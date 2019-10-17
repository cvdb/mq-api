package com.transixs.mq;

import java.util.List;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TestMessageProcessorFactory implements IMessageProcessorFactory {

  private static final Logger log = LogManager.getLogger(TestMessageProcessorFactory.class.getName());
  private long sleepTime;
  private String name;

  public TestMessageProcessorFactory(long sleepTime, String name) {
    this.sleepTime = sleepTime;
    this.name = name;
  }

  public IMessageProcessor getMessageProcessor() {
    return new TestMessageProcessor(sleepTime, name);
  }

}
