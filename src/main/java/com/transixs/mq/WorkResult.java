package com.transixs.mq;

public class WorkResult {

  private String result = "";
  private boolean completed = false;

  private WorkResult() {
  }

  //NOTE: async calls don't expect a response
  public static WorkResult COMPLETE() {
    WorkResult workResult = new WorkResult();
    workResult.setCompleted(true);
    return workResult;
  }

  public static WorkResult COMPLETE(String result) {
    WorkResult workResult = new WorkResult();
    workResult.setResult(result);
    workResult.setCompleted(true);
    return workResult;
  }

  // NOTE: this may not be a good idea....
  // messages that are NOT acknowledged will only
  // be re-deilivered if the consumer DISCONNECTS....
  public static WorkResult INCOMPLETE() {
    WorkResult workResult = new WorkResult();
    return workResult;
  }

  public void setResult(String result) {
    this.result = result;
  }

  public String getResult() {
    return result;
  }

  public void setCompleted(boolean completed) {
    this.completed = completed;
  }

  public boolean getCompleted() {
    return completed;
  }

}
