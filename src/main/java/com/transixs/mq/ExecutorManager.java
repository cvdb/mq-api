package com.transixs.mq;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public enum ExecutorManager {

  INSTANCE;

  private final Logger log = LogManager.getLogger(ExecutorManager.class.getName());

  /*****************************************************************************
   * There are 2 ways to run parallel executions: 1-threads, 2-more-instances
   * of each service.  The requirements are:
   *
   *  1. Don't waste resources. Keep services light weight.
   *  2. Take advantage of multi-core processors, usually 4 cores.
   *  3. Make it easy to configure i.e add or remove resources but not too many settings.
   *  4. Make it easy to tell that we need more resources. 
   *  5. Message Queue Brokers persist messages so don't cache within service queues.
   *
   *  Based on these requirements we don't want to have very large thread pools.
   *  5 is a good number to make use of multi-core processors. Also we use a direct hand-off
   *  queue so that if all worker threads are busy we get an exception that we can manage via code.
   *
   *****************************************************************************/

  // TODO: expose these in config 
  private final int corePoolSize = 1;
  private final int maximumPoolSize = 5;
  private final int keepAliveTime = 5; // MINUTES

  public ExecutorService getExecutor() {
    SynchronousQueue workQueue = new SynchronousQueue();
    return new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, TimeUnit.MINUTES, workQueue);
  }

  public void shutdown(ExecutorService executor) {
    try {
      log.warn("attempt to shutdown executor");
      executor.shutdown();
      executor.awaitTermination(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      log.warn("tasks interrupted");
    } finally {
      if (!executor.isTerminated()) {
        log.warn("cancel non-finished tasks");
      }
      executor.shutdownNow();
      log.warn("shutdown finished");
    }
  }

}
