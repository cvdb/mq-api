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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.RejectedExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ReceiverShared extends Thread {

  private final Logger log = LogManager.getLogger(ReceiverShared.class.getName());
  private IWorkerFactory workerFactory;
  private PulsarClient client;
  private Consumer consumer;
  private String consumerName;
  private boolean doStop = false;
  private final ExecutorService executor = ExecutorManager.INSTANCE.getExecutor();
  private Map<String, Producer> producers = new HashMap<>();

  public ReceiverShared(PulsarClient client, Consumer consumer, IWorkerFactory workerFactory) {
    this.client = client;
    this.consumer = consumer;
    this.consumerName = PulsarFactory.INSTANCE.getSharedConsumerName();
    this.workerFactory = workerFactory;
    WorkerManager.validateWorkerFactory(workerFactory);
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
        executeWork(msg);
      } while (!doStop);
      log.warn("shutdown for consumer : " + consumerName);
    } catch(Exception e){
      log.error("Error in consumer : " + consumerName, e);
      throw new MessageQueueException("Error in consumer : " + consumerName, e);
    }
  }

  //=========================================================================
  // NB THIS IS VERY IMPORTANT
  // -------------------------
  // This code BLOCKS until resources are available to do the work.
  // It also reports via an exception if there are not enough resources.
  // The idea is that we want the system to process ALL work but if we run low
  // on resources the work will simply take longer to process.
  // An alert can be setup to monitor this exception
  //=========================================================================
  private void executeWork(Message<byte[]> msg) {
    boolean taskAccepted = false;
    do {
      try {
        if (msg != null && !doStop) {
          executor.execute(() -> processMessage(msg));
        }
        taskAccepted = true;
      } catch(RejectedExecutionException rege){
        log.error("INSUFFICIENT-RESOURCES - Failed to submit task to executor in consumer : " + consumerName, rege);
        sleep();
      }
    } while (!taskAccepted);
  }

  private void sleep() {
    try {
      Thread.sleep(100);
    } catch(Exception e){
      e.printStackTrace();
    }
  }

  private void processMessage(Message<byte[]> msg) {
    try {
      // NOTE: doStop may have been triggered WHILE the thread was blocked by consumer.receive() call.
      // so make sure we don't process the message if we should be stopping
      if (doStop) {
        return;
      }
      String content = new String(msg.getData());
      String resource = msg.getProperty(MessageProperties.REQUEST_RESOURCE.name());
      String action = msg.getProperty(MessageProperties.REQUEST_ACTION.name());
      log.info("Received message '{}' for resource '{}', action '{}', message ID {} in consumer {}", content, resource, action, msg.getMessageId(), consumerName);
      WorkResult result = WorkerManager.doWork(resource, action, workerFactory, content);
      log.info("Received work result for resource '{}', action '{}', message ID {} in consumer {}", resource, action, msg.getMessageId(), consumerName);
      completeMessage(result, msg);
    } catch(Exception e){
      log.error("failed to process message for consumer : " + consumerName, e);
      //////////////////////////////////////////////////////////////////////////////////////////
      // DON'T THROW this exception. It would cause the processing thread to stop.
      //////////////////////////////////////////////////////////////////////////////////////////
    }
  }

  private void completeMessage(WorkResult workResult, Message<byte[]> msg) {
    if (workResult.getCompleted()) {
      try {
        consumer.acknowledge(msg);
      } catch(Exception e){
        log.error("failed to acknowledge message for consumer : " + consumerName, e);
        throw new MessageQueueException("failed to acknowledge message for consumer : " + consumerName, e);
      }
    } else {
      log.error("Worker failed to complete its work with ID {} in consumer {}", msg.getMessageId(), consumerName);
      // May be better to throw an exception here to aoid trying to send a response
    }

    try {
      // NOW, if this message has a REPLY TO QUEUE we need to send the response back
      // NOTE: it is assume here that if the worker failed to complete its work
      // its because of some system error, not a know error the worker could deal with.
      // In this case not sure sending back ANY response here is wort it?
      //
      if (msg.hasProperty(MessageProperties.REPLY_TO_TOPIC_URL.name())) {
        // If the message has a REPLY-TO queue then the caller expects a response.
        // If the service did not return one then its a problem
        if (workResult.getResult() == null) {
          throw new Exception("caller expects a response but service returned VOID");
        }
        MessageId msgId = getProducer(msg.getProperty(MessageProperties.REPLY_TO_TOPIC_URL.name())).newMessage()
          .value(workResult.getResult().getBytes())
          .property(MessageProperties.REQUEST_MESSAGE_ID.name(), msg.getMessageId().toString())
          .send();
        log.info("Published response to message ID {} with the ressponse ID {}", msg.getMessageId(), msgId);
      }

    } catch(Exception e){
      log.error("failed to send message response for consumer : " + consumerName, e);
      throw new MessageQueueException("failed to send message response for consumer : " + consumerName, e);
    }
  }

  private synchronized Producer getProducer(String topicName) {
    if (producers.containsKey(topicName)) {
      return producers.get(topicName); 
    } else {
      Producer p = PulsarFactory.INSTANCE.getProducer(client, topicName);
      producers.put(topicName, p);
      return p;
    }
  }

  // We want to make sure all worker threads have completed their work
  // before we shutdown
  public synchronized void stopit() {
    doStop = true;
    ExecutorManager.INSTANCE.shutdown(executor);
  }

}
