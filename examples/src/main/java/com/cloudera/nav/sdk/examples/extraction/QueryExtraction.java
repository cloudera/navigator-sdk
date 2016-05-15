package com.cloudera.nav.sdk.examples.extraction;

import com.cloudera.nav.sdk.client.ClientConfig;
import com.cloudera.nav.sdk.client.ClientConfigFactory;
import com.cloudera.nav.sdk.client.MetadataExtractor;
import com.cloudera.nav.sdk.client.MetadataResultIterator;
import com.cloudera.nav.sdk.client.MetadataResultSet;
import com.cloudera.nav.sdk.client.NavApiCient;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.opencsv.CSVWriter;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryExtraction {
  private static final Logger LOG =
      LoggerFactory.getLogger(QueryExtraction.class);

  private static Integer limit = 500000;

  static class OperationExecution {
    String identity;
    long duration;
    Instant startTime;
    String principal;
    private Instant minStartTime;
    private Instant maxEndTime;
    private List<String> mrStartTimes = Lists.newArrayList();
    private List<String> mrEndTimes = Lists.newArrayList();
    int numStages;

    public OperationExecution(Map<String, Object> obj) {
      identity = (String) obj.get("identity");
      Object started = obj.get("started");
      if (started != null) {
        startTime = Instant.parse((String) obj.get("started"));
      }
      principal = (String) obj.get("principal");
    }

    public void addMrJob(Map<String, Object> mrJobExec) {
      minStartTime = getMinTime(minStartTime, mrJobExec.get("started"));
      maxEndTime = getMaxTime(maxEndTime, mrJobExec.get("ended"));
      this.mrStartTimes.add((String) mrJobExec.get("started"));
      this.mrEndTimes.add((String) mrJobExec.get("ended"));
      numStages++;
    }

    public void computeDuration() {
      if (minStartTime != null && maxEndTime != null) {
        duration = maxEndTime.minus(minStartTime.getMillis()).getMillis();
      }

    }
  }

  static class Operation {
    String queryText;
    String identity;
    Instant lastStarted;
    String lastPrincipal;

    TreeSet<Long> durations = Sets.newTreeSet();
    TreeSet<Instant> startTimes = Sets.newTreeSet();
    TreeSet<String> users = Sets.newTreeSet();

    long minDuration;
    long maxDuration;
    long averageDuration;
    long medianDuration;
    List<OperationExecution> executions = Lists.newArrayList();
    private String minStartTime;
    private String maxStartTime;
    private String userString;
    private int maxStages;

    public Operation(String opId) {
      this.identity = opId;

    }

    public void addOperationInstances1(List<String> execIds, Map<String,
        OperationExecution> allOpExecs) {

      for(String execId : execIds) {
        OperationExecution operationExecution = allOpExecs.get(execId);
        if (lastStarted == null || lastStarted.isBefore(operationExecution.startTime)) {
          lastStarted = operationExecution.startTime;
          lastPrincipal = operationExecution.principal;
        }
        durations.add(operationExecution.duration);
        averageDuration += operationExecution.duration;
        executions.add(operationExecution);
      }
    }

    public void addOperationInstances(List<String> execIds, Map<String,
        OperationExecution> allOpExecs) {
      for(String execId : execIds) {
        OperationExecution operationExecution = allOpExecs.get(execId);
        executions.add(operationExecution);
      }
    }

    public void computeDurations() {
      for(OperationExecution operationExecution : executions) {
        durations.add(operationExecution.duration);
        averageDuration += operationExecution.duration;

        if (operationExecution.startTime != null) {
          startTimes.add(operationExecution.startTime);
        }
        if (operationExecution.principal != null) {
          users.add(operationExecution.principal);
        }
        maxStages =
            maxStages > operationExecution.numStages ? maxStages : operationExecution.numStages;
      }

      minDuration = durations.first();
      maxDuration = durations.last();
      medianDuration = Iterables.get(durations, (durations.size() - 1) / 2);
      averageDuration = averageDuration/executions.size();

      minStartTime = startTimes.isEmpty() ? "" : startTimes.first().toString();
      maxStartTime = startTimes.isEmpty() ? "" : startTimes.last().toString();
      userString = Joiner.on(",").skipNulls().join(users);
    }

    public void writeOptimizerFormat(int index, CSVWriter csvWriter) {
      String queryType = getQueryType(escapeQueryText(queryText));

      csvWriter.writeNext(new String[] {Integer.toString(index++),
          identity,
          Integer.toString(executions.size()),
          Long.toString(minDuration),Long.toString(maxDuration),
          Long.toString(averageDuration),Long.toString(medianDuration),
          minStartTime, maxStartTime, Integer.toString(maxStages),
          userString, queryType, queryText});
    }

    public void writeSigmaFormat(int index, CSVWriter csvWriter) {
      for (OperationExecution oe : executions) {
        for (int i = 0; i < oe.mrEndTimes.size(); i++) {
          if (oe.mrStartTimes.get(i) == null ||
              oe.mrEndTimes.get(i) == null) {
            continue;
          }

          csvWriter.writeNext(new String[] {
              oe.identity,
              Long.toString(Instant.parse(oe.mrStartTimes.get(i)).getMillis()),
              Long.toString(Instant.parse(oe.mrEndTimes.get(i)).getMillis()),
              escapeQueryText(queryText)
          });
        }
      }
    }

    public void write(int i, CSVWriter sigmaCSVWriter, CSVWriter
        optimizerCsvWriter) {
      writeSigmaFormat(i, sigmaCSVWriter);
      writeOptimizerFormat(i, optimizerCsvWriter);
    }

    public static void writeHeaders(CSVWriter sigmaCSVWriter,
                                    CSVWriter optimizerCsvWriter) {
      sigmaCSVWriter.writeNext(new String[] {
          "QueryExecIdentity, mrStartTime, mrEndTime, query"});
      optimizerCsvWriter.writeNext(new String[] {
          "index, QueryIdentity, totalExecutions, minDuration, maxDuration, averageDuration, medianDuration," +
              "users, minStartTime, maxStartTime, queryType"});
    }
  }

  @SuppressWarnings("unchecked")
  public static  void main(String[] args) throws IOException {
    LogManager.getRootLogger().setLevel(Level.INFO);

    String configFilePath = args[0];
    ClientConfig config = (new ClientConfigFactory())
        .readConfigurations(configFilePath);

    // Initialize the API.
    NavApiCient client = new NavApiCient(config);
    MetadataExtractor extractor = new MetadataExtractor(client, limit);

    // Collect all the operation executions.
    String entityQuery = "sourceType:HIVE AND (type:operation_execution)";

//    String entityQuery =
//        "+(sourceType:HIVE AND (type:operation_execution)) +(+sourceType:hive +started:[NOW/DAY-5DAYS TO NOW/DAY+1DAY])";

    // GEt all the op execs and their time, user
    MetadataResultSet resultSet = null;
    MetadataResultIterator iterator = null;

    Map<String, OperationExecution> opExecs = Maps.newHashMap();
    resultSet = extractor.extractMetadata(null, null, entityQuery, null);
    iterator = resultSet.getEntities().iterator();
    while(iterator.hasNext()) {
      Map<String, Object> obj = iterator.next();
      OperationExecution operationExecution =
          new OperationExecution(obj);
      opExecs.put(operationExecution.identity, operationExecution);

      if (opExecs.size()%1000 == 0) {
        LOG.info("Processed {} operation executions", opExecs.size());
      }
    }

    LOG.info("Obtained {} operation executions", opExecs.size());

    // Collect MR operations so that we can collect the elapsed time.
    collectElapsedTimes(extractor, opExecs);

    // Collect operation Ids.
    Map<String, Operation> operations = Maps.newHashMap();

    int index = 0;
    Iterable<List<String>> partitions = Iterables.partition
        (opExecs.keySet(), 10000);
    for(List<String> partition : partitions) {
      LOG.info("Processed {} operation executions", index);

      String entityIds = Joiner.on(",").join(partition);
      String relationsQuery = "type:INSTANCE_OF AND {!terms f=endpoint2Ids}" + entityIds + ",dummy";
      resultSet = extractor.extractMetadata(null, null, null, relationsQuery);
      iterator = resultSet.getRelations().iterator();
      while(iterator.hasNext()) {
        Map<String, Object> obj = iterator.next();
        Map<String, Object> template = (Map<String, Object>) obj.get("template");
        String opId = (String) template.get("entityId");

        Map<String, Object> instances = (Map<String, Object>) obj.get("instances");
        List<String> execIds = (List<String>) instances.get("entityIds");

        Operation operation = operations.get(opId);
        if (operation == null) {
          operation = new Operation(opId);
          operations.put(opId, operation);
        }
        operation.addOperationInstances(execIds, opExecs);

        index++;
      }
    }

    for (Operation operation : operations.values()) {
      operation.computeDurations();
    }

    index = 0;
    LOG.info("Obtained {} operations", operations.size());
    partitions = Iterables.partition(operations.keySet(), 10000);
    for(List<String> partition : partitions) {
      LOG.info("Processed {} operations", index);
      // Get the operation Ids now.
      String entityIds = Joiner.on(",").join(partition);
      entityQuery = "{!terms f=identity}" + entityIds + ",dummy";
      resultSet = extractor.extractMetadata(null, null, entityQuery, null);
      iterator = resultSet.getEntities().iterator();
      while(iterator.hasNext()) {
        Map<String, Object> obj = iterator.next();
        String queryText = (String) obj.get("queryText");
        String identity = (String) obj.get("identity");

        Operation operation = operations.get(identity);
        operation.queryText = queryText == null ? "" : queryText;
      }
    }

    String sigmaOutputFile = args[1];
    String optimizerOutputFile = args[2];

    PrintWriter sigmaWriter = new PrintWriter(sigmaOutputFile, "UTF-8");
    CSVWriter sigmaCSVWriter =
        new CSVWriter(sigmaWriter, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.DEFAULT_QUOTE_CHARACTER, "\n");

    PrintWriter optimizerWriter = new PrintWriter(optimizerOutputFile, "UTF-8");
    CSVWriter optimizerCsvWriter =
        new CSVWriter(optimizerWriter, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.DEFAULT_QUOTE_CHARACTER, "@@@@@");
        //new CSVWriter(optimizerWriter, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.DEFAULT_QUOTE_CHARACTER, "\n");

    Operation.writeHeaders(sigmaCSVWriter, optimizerCsvWriter);

    for(Operation operation : operations.values()) {
      operation.write(index++, sigmaCSVWriter, optimizerCsvWriter);
    }

    LOG.info("Processed {} operations", index);

    sigmaCSVWriter.close();
    optimizerCsvWriter.close();
  }

  @SuppressWarnings({ "unused", "unchecked" })
  private static void collectElapsedTimes(MetadataExtractor extractor,
                                          Map<String, OperationExecution> opExecs) {
    LOG.info("Processing elapsed times");

    // First collect all MRIds.
    Map<String, OperationExecution> mrIdToOpExecs = Maps.newHashMap();

    int index = 0;
    Iterable<List<String>> partitions = Iterables.partition
        (opExecs.keySet(), 10000);
    for(List<String> partition : partitions) {
      LOG.info("Processed {} operation executions", index);

      String entityIds = Joiner.on(",").join(partition);
      String relationsQuery = "type:LOGICAL_PHYSICAL AND {!terms f=endpoint1Ids}" + entityIds + ",dummy";

      MetadataResultSet resultSet = extractor.extractMetadata(null, null,
          null, relationsQuery);
      MetadataResultIterator iterator = resultSet.getRelations().iterator();
      while(iterator.hasNext()) {
        Map<String, Object> relation = iterator.next();
        Map<String, Object> ep1 = (Map<String, Object>) relation.get("logical");
        String opExecId = (String) ep1.get("entityId");

        Map<String, Object> ep2 = (Map<String, Object>) relation.get("physical");
        List<String> mrExecIds = (List<String>) ep2.get("entityIds");

        for(String mrId : mrExecIds) {
          mrIdToOpExecs.put(mrId, opExecs.get(opExecId));
        }
        index++;
      }
    }

    LOG.info("Obtained {} MR job executions", mrIdToOpExecs.size());

    index = 0;
    // Now collect all the entities for these MRIds.
    partitions = Iterables.partition(mrIdToOpExecs.keySet(), 10000);
    for(List<String> partition : partitions) {
      LOG.info("Processed {} Mr job executions", index);

      String entityIds = Joiner.on(",").join(partition);
      String entitisQuery = "{!terms f=identity}" + entityIds + ",dummy";
      MetadataResultSet resultSet = extractor.extractMetadata(null, null,
          entitisQuery, null);
      MetadataResultIterator iterator = resultSet.getEntities().iterator();
      while(iterator.hasNext()) {
        Map<String, Object> mrJobExec = iterator.next();
        String mrId = (String) mrJobExec.get("identity");

        OperationExecution opExecution = mrIdToOpExecs.get(mrId);
        opExecution.addMrJob(mrJobExec);
        index++;
      }
    }

    for(OperationExecution operationExecution : opExecs.values()) {
      operationExecution.computeDuration();
    }

    LOG.info("Finished computing elapsed times");
  }

  private static Instant getMaxTime(Instant maxEndTime, Object ended) {
    if (ended == null) {
      return maxEndTime;
    }

    Instant endTime = Instant.parse((String) ended);
    if (maxEndTime == null) {
      return endTime;
    }
    return endTime.isAfter(maxEndTime) ? endTime : maxEndTime;
  }

  private static Instant getMinTime(Instant minStartTime, Object started) {
    if (started == null) {
      return minStartTime;
    }

    Instant startTime = Instant.parse((String) started);
    if (minStartTime == null) {
      return startTime;
    }
    return startTime.isBefore(minStartTime) ? startTime : minStartTime;
  }

  private static String escapeQueryText(String queryText) {
    queryText = queryText.replaceAll("\"", "\\\"");
    return queryText.replaceAll("\n", " ");
  }

  private static String getQueryType(String queryText) {
    queryText = queryText.trim().toLowerCase();
    if (queryText.startsWith("insert")) {
      return "insert";
    } else if (queryText.startsWith("create")) {
      return "create";
    } else if (queryText.startsWith("delete")) {
      return "delete";
    } else if (queryText.startsWith("select")) {
      return "select";
    } else {
      return "unknown";
    }
  }
}