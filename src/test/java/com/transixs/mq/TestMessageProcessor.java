package com.transixs.mq;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TestMessageProcessor implements IMessageProcessor{

  private static final Logger log = LogManager.getLogger(TestMessageProcessor.class.getName());
  private long sleepTime;
  private String name;

  public TestMessageProcessor(long sleepTime, String name) {
    this.sleepTime = sleepTime;
    this.name = name;
  }

  public void process(String message) {
    log.warn("message processor:" + name + " received work:" + message);
    sleep();
    log.warn("message processor:" + name + " completed work:" + message);
  }

  private void sleep() {
    try {
      Thread.sleep(sleepTime);
    } catch(Exception e){
      e.printStackTrace();
    }
  }

}
