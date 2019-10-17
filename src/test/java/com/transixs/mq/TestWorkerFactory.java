package com.transixs.mq;

import java.util.List;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TestWorkerFactory implements IWorkerFactory {

  private static final Logger log = LogManager.getLogger(TestWorkerFactory.class.getName());

  public Object getWorker(Class clazz) {
    return new TestWorker();
  }

  public List<Class> getWorkerClasses() {
    return Arrays.asList(TestWorker.class);
  }

}
