package com.transixs.mq;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Resource("resource")
public class TestWorker {

  private static final Logger log = LogManager.getLogger(TestWorker.class.getName());

  @Action("action")
  public WorkResult doWork(String message) {
    try {
      log.warn("received work:" + message);
      // Thread.sleep(1000);
      log.warn("work completed:" + message);

      //=====================================================================
      // NOTE: a COMPLETE work result means the message will be acknowledged
      // so it will not be delivered to another worker
      //=====================================================================
      return WorkResult.COMPLETE(message + " - completed");

    } catch(Exception e){
      e.printStackTrace();

      //=============================================================================
      // NOTE: an INCOMPLETE work result means the message will NOT be acknowledged
      // so it will be delivered to another worker
      //=============================================================================
      return WorkResult.INCOMPLETE();
    }
  }

}
