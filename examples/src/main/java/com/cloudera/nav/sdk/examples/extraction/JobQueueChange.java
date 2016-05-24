package com.cloudera.nav.sdk.examples.extraction;


public class JobQueueChange {
  public String type;
  public Event event;

  public static class JobQueueChangeEvent {
    public String jobQueueName;
  }

  public static class Event {
    public JobQueueChangeEvent JobQueueChange;
  }
}
