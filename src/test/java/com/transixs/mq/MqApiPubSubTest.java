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
class MqApiPubSubTest {

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
      MessageQueueAPI.INSTANCE.receivePubSub(new TestMessageProcessorFactory(500, "fast-receiver"), "some-topic");
      MessageQueueAPI.INSTANCE.receivePubSub(new TestMessageProcessorFactory(1000, "average-receiver"), "some-topic");
      MessageQueueAPI.INSTANCE.receivePubSub(new TestMessageProcessorFactory(2000, "slow-receiver"), "some-topic");

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
  void send_receive_async() {
    MessageQueueAPI.INSTANCE.sendAsyncPubSub("some-topic", "test ASYNC PUB-SUB work 1");
    MessageQueueAPI.INSTANCE.sendAsyncPubSub("some-topic", "test ASYNC PUB-SUB work 2");
    MessageQueueAPI.INSTANCE.sendAsyncPubSub("some-topic", "test ASYNC PUB-SUB work 3");
    MessageQueueAPI.INSTANCE.sendAsyncPubSub("some-topic", "test ASYNC PUB-SUB work 4");
    MessageQueueAPI.INSTANCE.sendAsyncPubSub("some-topic", "test ASYNC PUB-SUB work 5");

    // longest processor will take 10 seconds...so sleep for 12
    sleep();
    sleep();
    sleep();
    sleep();
    sleep();
    sleep();
    System.out.println("done sleeping....");

  }

  private static void sleep() {
    try {
      Thread.sleep(2000);
    } catch(Exception e){
      e.printStackTrace();
    }
  }

}
