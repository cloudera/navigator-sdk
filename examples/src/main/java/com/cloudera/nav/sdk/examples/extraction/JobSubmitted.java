package com.cloudera.nav.sdk.examples.extraction;


public class JobSubmitted {

  public String type;
  public Event event;

  public static class JobSubmitEvent {
    public String userName;
    public long submitTime;
    public String jobid;
  }

  public static class Event {
    public JobSubmitEvent JobSubmitted;
  }
}
