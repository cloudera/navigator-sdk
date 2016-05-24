package com.cloudera.nav.sdk.examples.extraction;


public class JobFinished {
  public String type;
  public Event event;

  public static class JobFinishedEvent {
    public long finishTime;
    public long finishedMaps;
    public long finishedReduces;
    public long failedMaps;
    public long failedReduces;
  }

  public static class Event {
    public JobFinishedEvent JobFinished;
  }
}
