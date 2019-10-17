package com.transixs.mq;

import java.util.Map;
import java.util.HashMap;

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.SubscriptionType;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ReceiverExclusive extends Thread {

  private final Logger log = LogManager.getLogger(ReceiverExclusive.class.getName());
  private Consumer consumer;
  private String consumerName;
  private boolean doStop = false;
  private Map<String, CountDownLatch> latches = new HashMap<>();
  private Map<String, String> responses = new HashMap<>();

  public ReceiverExclusive(Consumer consumer) {
    this.consumer = consumer;
    this.consumerName = PulsarFactory.INSTANCE.getExclusiveConsumerName(consumer);
  }

  public void run() {
    try {
      log.warn("starting for consumer : " + consumerName);
      int counter = 0; 
      do {
        counter++;
        if (counter > 11) { // 120 seconds
          log.info("waiting for messages for consumer : " + consumerName);
          counter = 0;
        }
        Message<byte[]> msg = consumer.receive(10, TimeUnit.SECONDS);
        if (msg != null && !doStop) {
          processMessage(msg);
        }
      } while (!doStop);
      log.warn("shutdown for consumer : " + consumerName);
    } catch(Exception e){
      log.error("Error in consumer : " + consumerName, e);
      throw new MessageQueueException("Error in consumer : " + consumerName, e);
    }
  }

  private void processMessage(Message<byte[]> msg) {
    try {
      // NOTE: doStop may have been triggered WHILE the thread was blocked by consumer.receive() call.
      // so make sure we don't process the message if we should be stopping
      if (doStop) {
        return;
      }

      consumer.acknowledge(msg); // Always acknowledge in this case
      String content = new String(msg.getData());
      log.info("Received message with ID {} in response to message ID {} in consumer {}", msg.getMessageId(), msg.getProperty(MessageProperties.REQUEST_MESSAGE_ID.name()), consumerName);
      
      responses.put(msg.getProperty(MessageProperties.REQUEST_MESSAGE_ID.name()), content);
      CountDownLatch latch = tryGetLatchOrTimeout(msg);
      if (latch == null) {
        throw new Exception("no latch found for message ID:" + msg.getProperty(MessageProperties.REQUEST_MESSAGE_ID.name()));  
      }
      latch.countDown();

    } catch(Exception e){
      log.error("failed to process message for consumer : " + consumerName, e);
      //////////////////////////////////////////////////////////////////////////////////////////
      // DON'T THROW this exception. It would cause the processing thread to stop.
      //////////////////////////////////////////////////////////////////////////////////////////
    }
  }

  // The message ID is not know until we get confirmation that the message has actually
  // been sent in the sender. So in some cases the response may arrive before the latch
  // has been setup. So we need to make sure we allow some time before we fail.
  private CountDownLatch tryGetLatchOrTimeout(Message<byte[]> msg) {
    CountDownLatch latch = null;
    long awaitSeconds = Long.parseLong(ServiceConfig.SYNC_RESPONSE_TIMEOUT_SECONDS.getValue()); 
    Instant first = Instant.now();
    Instant second = Instant.now();
    do {
      latch = latches.get(msg.getProperty(MessageProperties.REQUEST_MESSAGE_ID.name()));
      if (latch == null) {
        sleep();
      }
      second = Instant.now();
    } while ((Duration.between(first, second).getSeconds() < awaitSeconds) && latch == null);
    return latch;
  }

  private void sleep() {
    try {
      Thread.sleep(50);
    } catch(Exception e){
      e.printStackTrace();
    }
  }

  public String await(MessageId msgId) {
    try {
      log.info("Awaiting message with ID {} in consumer {}", msgId, consumerName);
      CountDownLatch waiter = new CountDownLatch(1);
      latches.put(msgId.toString(), waiter);
      long awaitSeconds = Long.parseLong(ServiceConfig.SYNC_RESPONSE_TIMEOUT_SECONDS.getValue()); 
      if (waiter.await(awaitSeconds, TimeUnit.SECONDS)) {
        return responses.get(msgId.toString()); 
      } else {
        throw new MessageQueueException("latch await timeout for message ID: " + msgId);
      }
    } catch(Exception e){
      log.error("failed to await message for consumer : " + consumerName, e);
      throw new MessageQueueException("failed to await message for consumer : " + consumerName, e);
    } finally {
      try {
        latches.remove(msgId.toString()); 
        responses.remove(msgId.toString()); 
      } catch(Exception e){
        log.error("failed to perform message cleanup for consumer : " + consumerName, e);
      }
    }
  }

  public String getTopic() {
    return consumer.getTopic();
  }

  public synchronized void stopit() {
    doStop = true;
  }

}
