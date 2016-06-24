package com.cloudera.nav.sdk.examples.extraction;

/**
 * A set of configuration options needed by QueryExtraction example
 */
public class QueryExtractionConfig {
  private String outputDirectory;
  private String startTime;
  private String endTime;
  private String duration;
  private String principal;
  private String operationExecutionQuery;
  private String optimizerDelimiter;

  public String getPrincipal() {
    return principal;
  }

  public void setPrincipal(String principal) {
    this.principal = principal;
  }

  public String getOperationExecutionQuery() {
    return operationExecutionQuery;
  }

  public void setOperationExecutionQuery(String operationExecutionQuery) {
    this.operationExecutionQuery = operationExecutionQuery;
  }

  public String getOutputDirectory() {
    return outputDirectory;
  }

  public void setOutputDirectory(String outputDirectory) {
    this.outputDirectory = outputDirectory;
  }

  public String getStartTime() {
    return startTime;
  }

  public void setStartTime(String startTime) {
    this.startTime = startTime;
  }

  public String getEndTime() {
    return endTime;
  }

  public void setEndTime(String endTime) {
    this.endTime = endTime;
  }

  public String getDuration() {
    return duration;
  }

  public void setDuration(String duration) {
    this.duration = duration;
  }

  public String getOptimizerDelimiter() {
    return optimizerDelimiter;
  }

  public void setOptimizerDelimiter(String optimizerDelimiter) {
    this.optimizerDelimiter = optimizerDelimiter;
  }
}
