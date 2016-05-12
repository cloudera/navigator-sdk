package com.cloudera.nav.sdk.examples.extraction;

import com.cloudera.nav.sdk.client.ClientConfig;
import com.cloudera.nav.sdk.client.ClientConfigFactory;
import com.cloudera.nav.sdk.client.MetadataExtractor;
import com.cloudera.nav.sdk.client.MetadataResultIterator;
import com.cloudera.nav.sdk.client.MetadataResultSet;
import com.cloudera.nav.sdk.client.NavApiCient;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.opencsv.CSVWriter;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryExtraction {
  private static final Logger LOG =
      LoggerFactory.getLogger(QueryExtraction.class);
  private static final Set SOURCE_TYPES = Sets.newHashSet();

  private static Integer limit = 500000;

  static class OperationExecution {
    String identity;
    long duration;
    Instant startTime;
    String principal;
    private Instant minStartTime;
    private Instant maxEndTime;

    public OperationExecution(MetadataExtractor extractor, Map<String, Object> obj) {
      identity = (String) obj.get("identity");
      Object started = obj.get("started");
      if (started != null) {
        startTime = Instant.parse((String) obj.get("started"));
      }
      principal = (String) obj.get("principal");

//      Instant minStartTime = null, maxEndTime = null;
//      String relationsQuery = "type:LOGICAL_PHYSICAL AND endpoint1Ids:" + identity;
//
//      MetadataResultSet resultSet = extractor.extractMetadata(null, null,
//          null, relationsQuery);
//      IncrementalExtractIterator iterator = resultSet.getRelations().iterator();
//      while(iterator.hasNext()) {
//        obj = iterator.next();
//
//        Map<String, Object> instances = (Map<String, Object>) obj.get("physical");
//        List<String> physicalIds = (List<String>) instances.get("entityIds");
//
//        StringBuilder query = new StringBuilder("(");
//        for(String id : physicalIds) {
//          query.append("identity:" + id + " OR ");
//        }
//        query.append("identity:dummy)");
//
//        MetadataResultSet physicalResultSet = extractor.extractMetadata(null,
//            null, query.toString(), null);
//        IncrementalExtractIterator physicalEntities = physicalResultSet
//            .getEntities().iterator();
//        while(physicalEntities.hasNext()) {
//          Map<String, Object> mrEntity = physicalEntities.next();
//          minStartTime = getMinTime(minStartTime, mrEntity.get("started"));
//          maxEndTime = getMaxTime(maxEndTime, mrEntity.get("ended"));
//        }
//      }
//
//      if (minStartTime != null && maxEndTime != null) {
//        duration = maxEndTime.minus(minStartTime.getMillis()).getMillis();
//      }
    }

    public void addMrJob(Map<String, Object> mrJobExec) {
      minStartTime = getMinTime(minStartTime, mrJobExec.get("started"));
      maxEndTime = getMaxTime(maxEndTime, mrJobExec.get("ended"));
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

    int totalExecutions;

    TreeSet<Long> durations = Sets.newTreeSet();

    long minDuration;
    long maxDuration;
    long averageDuration;
    long medianDuration;

    public Operation(String opId) {
      this.identity = opId;

    }

    public void addOperationInstances(List<String> execIds, Map<String,
        OperationExecution> opExecs) {
      totalExecutions += execIds.size();


      for(String execId : execIds) {
        OperationExecution operationExecution = opExecs.get(execId);
        if (lastStarted == null || lastStarted.isBefore(operationExecution.startTime)) {
          lastStarted = operationExecution.startTime;
          lastPrincipal = operationExecution.principal;
        }
        durations.add(operationExecution.duration);
        averageDuration += operationExecution.duration;
      }
      minDuration = durations.first();
      maxDuration = durations.last();
      medianDuration = Iterables.get(durations, (durations.size() - 1) / 2);
      averageDuration = averageDuration/execIds.size();
    }

    public void write(int index, CSVWriter csvWriter) {
      String queryType = getQueryType(escapeQueryText(queryText));

      csvWriter.writeNext(new String[] {Integer.toString(index++),
          identity,
          Integer.toString(totalExecutions),
          Long.toString(minDuration),Long.toString(maxDuration),
          Long.toString(averageDuration),Long.toString(medianDuration),
          lastPrincipal, lastStarted != null ? lastStarted.toString() : "", queryType, queryText});

    }
  }

  public static  void main(String[] args) throws IOException {
    LogManager.getRootLogger().setLevel(Level.INFO);

    String configFilePath = args[0];
    ClientConfig config = (new ClientConfigFactory())
        .readConfigurations(configFilePath);

    // Initialize the API.
    NavApiCient client = new NavApiCient(config);
    MetadataExtractor extractor = new MetadataExtractor(client, limit);

    // Whats my starting marker?
//    String marker;
//    try {
//      String markerReadPath = args[1];
//      File markerFile = new File(markerReadPath);
//      FileReader fr = new FileReader(markerFile);
//      BufferedReader markerReader = new BufferedReader(fr);
//      marker = markerReader.readLine();
//      markerReader.close();
//      fr.close();
//    } catch (IOException e) {
//      throw Throwables.propagate(e);
//    } catch (ArrayIndexOutOfBoundsException e){
//      marker=null;
//    }

    String outputFile = args[1];
    PrintWriter outputWriter = new PrintWriter(outputFile, "UTF-8");
    outputWriter.println(
        "identity, duration, queryType, query");


    // Collect all the operation executions.
    String entityQuery = "sourceType:IMPALA AND (type:operation_execution)";

    // GEt all the op execs and their time, user
    MetadataResultSet resultSet = null;
    MetadataResultIterator iterator = null;

    Map<String, OperationExecution> opExecs = Maps.newHashMap();
    resultSet = extractor.extractMetadata(null, null, entityQuery, null);
    iterator = resultSet.getEntities().iterator();
    while(iterator.hasNext()) {
      Map<String, Object> obj = iterator.next();
      OperationExecution operationExecution =
          new OperationExecution(extractor, obj);
      opExecs.put(operationExecution.identity, operationExecution);

      if (opExecs.size()%1000 == 0) {
        LOG.info("Processed {} operation executions", opExecs.size());
      }
    }

    LOG.info("Obtained {} operation executions", opExecs.size());

    // Collect MR operations so that we can collect the elapsed time.
    //collectElapsedTimes(extractor, opExecs);

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

    // Collect operation details like queryText, etc and write it out.
    CSVWriter csvWriter =
        new CSVWriter(outputWriter, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.DEFAULT_QUOTE_CHARACTER, "@@@@@");


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
        operation.write(index++, csvWriter);
      }
    }

    LOG.info("Processed {} operations", index);

    String nextMarker = "";
    //String nextMarker = getAllRelationships(extractor, marker);
    //String nextMarker = getSelectiveRelationships(extractor, marker, outputWriter);

    csvWriter.close();

    // Save the last marker so that it can be used for the next harvest.
    try {
      String markerWritePath = args[3];
      PrintWriter markerWriter = new PrintWriter(markerWritePath, "UTF-8");
      markerWriter.println(nextMarker);
      markerWriter.close();
    } catch(IOException e) {
      throw Throwables.propagate(e);
    } catch (ArrayIndexOutOfBoundsException e){
      LOG.error("Please specify a file to save next marker");
    }
  }

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
    return queryText.replaceAll("\r\n", "    ");
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