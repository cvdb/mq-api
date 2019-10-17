package com.transixs.mq;

import com.transixs.config.manager.EnvVarHelper;
import java.util.Map;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Disabled("Disabled - needs pulsar standalone to be running")
class MqApiTest {

  private static final Logger log = LogManager.getLogger(MqApiTest.class.getName());

  @BeforeAll
  static void initAll() {
    Map<String, String> newenv = new HashMap();

    newenv.put(ServiceConfig.SYNC_RESPONSE_TIMEOUT_SECONDS.name(), "10");
    newenv.put(ServiceConfig.SERVICE_NAME.name(), "testservice");
    newenv.put(ServiceConfig.PULSAR_SERVICE_URL.name(), "pulsar://localhost:6650");

    try {

      // Set ENV config
      EnvVarHelper.setEnv(newenv);

      // Initialize the MQ API
      MessageQueueAPI.INSTANCE.receive(new TestWorkerFactory());

    } catch(Exception e){
      e.printStackTrace();
    }
  }

  @AfterAll
  static void afterAll() {
    // sleep();
    MessageQueueAPI.INSTANCE.shutdown();
  }

  @Test
  void send_async() {
    MessageQueueAPI.INSTANCE.sendAsync("testservice", "resource", "action", "test ASYNC work 1");
    MessageQueueAPI.INSTANCE.sendAsync("testservice", "resource", "action", "test ASYNC work 2");
    MessageQueueAPI.INSTANCE.sendAsync("testservice", "resource", "action", "test ASYNC work 3");
  }

  @Test
  void send_receive_sync() {
    String response_1 = MessageQueueAPI.INSTANCE.sendSync("testservice", "resource", "action", "test SYNC work 1");
    String response_2 = MessageQueueAPI.INSTANCE.sendSync("testservice", "resource", "action", "test SYNC work 2");
    String response_3 = MessageQueueAPI.INSTANCE.sendSync("testservice", "resource", "action", "test SYNC work 3");

    assertEquals("test SYNC work 1 - completed", response_1);
    assertEquals("test SYNC work 2 - completed", response_2);
    assertEquals("test SYNC work 3 - completed", response_3);
  }
  
  private static void sleep() {
    try {
      Thread.sleep(2000);
    } catch(Exception e){
      e.printStackTrace();
    }
  }

}
