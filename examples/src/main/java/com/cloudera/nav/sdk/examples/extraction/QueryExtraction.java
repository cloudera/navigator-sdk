package com.cloudera.nav.sdk.examples.extraction;

import com.cloudera.nav.sdk.client.ClientConfig;
import com.cloudera.nav.sdk.client.ClientConfigFactory;
import com.cloudera.nav.sdk.client.MetadataExtractor;
import com.cloudera.nav.sdk.client.MetadataResultIterator;
import com.cloudera.nav.sdk.client.MetadataResultSet;
import com.cloudera.nav.sdk.client.NavApiCient;
import com.cloudera.nav.sdk.examples.extraction.HadoopConfiguration.ConfigProperty;
import com.cloudera.nav.sdk.examples.extraction.JobFinished.JobFinishedEvent;
import com.cloudera.nav.sdk.examples.extraction.JobQueueChange.JobQueueChangeEvent;
import com.cloudera.nav.sdk.examples.extraction.JobSubmitted.JobSubmitEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
import com.opencsv.CSVWriter;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryExtraction {
  private static final Logger LOG =
      LoggerFactory.getLogger(QueryExtraction.class);

  private static Integer limit = 500000;
  private static int jobCounter = 0;
  private static Marshaller marshaller;
  private static ObjectMapper mapper = new ObjectMapper();
  static {
      try {
        marshaller =
            JAXBContext.newInstance(HadoopConfiguration.class).createMarshaller();
      } catch (JAXBException e) {
        e.printStackTrace();
        System.exit(-1);
      }
  }
  static class OperationExecution {
    String identity;
    long duration;
    Instant startTime;
    String principal;
    private Instant minStartTime;
    private Instant maxEndTime;
    private Multimap<Long, Long> start2end = TreeMultimap.create();
    //private List<String> mrStartTimes = Lists.newArrayList();
    //private List<String> mrEndTimes = Lists.newArrayList();

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
      String start = (String) mrJobExec.get("started");
      String end = (String) mrJobExec.get("ended");
      minStartTime = getMinTime(minStartTime, mrJobExec.get("started"));
      maxEndTime = getMaxTime(maxEndTime, mrJobExec.get("ended"));
      if (start != null && end != null) {
        long startMs = Instant.parse(start).getMillis();
        long endMs = Instant.parse(end).getMillis();
        if (start2end.containsKey(startMs)) {
          LOG.warn("Duplicate start time");
        }
        start2end.put(startMs, endMs);
      }
      //this.mrStartTimes.add((String) mrJobExec.get("started"));
      //this.mrEndTimes.add((String) mrJobExec.get("ended"));
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
    List<OperationExecution> executions = Lists.newArrayList();

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
        executions.add(operationExecution);
      }
      minDuration = durations.first();
      maxDuration = durations.last();
      medianDuration = Iterables.get(durations, (durations.size() - 1) / 2);
      averageDuration = averageDuration/execIds.size();
    }

    private ConfigProperty makeProp(String name, String value) {
      ConfigProperty ret = new ConfigProperty();
      ret.name = name;
      ret.value = value;
      ret.source = "foo";
      return ret;
    }

    public void write(int index, CSVWriter csvWriter) {
      //String queryType = getQueryType(escapeQueryText(queryText));

      for (OperationExecution oe : executions) {
        int i = -1;
        for (Map.Entry<Long, Long> kv : oe.start2end.entries()) {
          i++;
          // Write conf file
          HadoopConfiguration cfg = new HadoopConfiguration();
          cfg.property = Lists.newArrayList();
          String jobId = "job_" + jobCounter;
          String dir = "/tmp/nav/" + Integer.toString(jobCounter/1000);
          File dirFile = new File(dir);
          if (!dirFile.exists()) dirFile.mkdirs();
          String jobName = "Stage" + Integer.toString(i + 1);
          cfg.property.add(makeProp("mapreduce.job.name", jobName));
          cfg.property.add(makeProp("hive.query.id", oe.identity));
          cfg.property.add(makeProp("hive.query.string", escapeQueryText(queryText)));
          File outfile = new File(dir, jobId + "_conf.xml");
          try {
            marshaller.marshal(cfg, outfile);
          } catch (JAXBException e) {
            e.printStackTrace();
            System.exit(-1);
          }
          long startMs = kv.getKey();
          long endMs = kv.getValue();

          // Write jhist file
          String jhistFileName = String.format(
              "%s-%d-%s-%s-%d-100-1-SUCCEEDED-root.hdfs-1462993272598.jhist",
              jobId, startMs, "user", jobName, endMs);
          JobSubmitted js = new JobSubmitted();
          js.type = "JOB_SUBMITTED";
          JobSubmitted.Event ev1 = new JobSubmitted.Event();
          ev1.JobSubmitted = new JobSubmitEvent();
          ev1.JobSubmitted.userName = "user";
          ev1.JobSubmitted.submitTime = startMs;
          ev1.JobSubmitted.jobid = jobId;
          js.event = ev1;

          JobQueueChange jq = new JobQueueChange();
          jq.type = "JOB_QUEUE_CHANGED";
          JobQueueChange.Event ev2 = new JobQueueChange.Event();
          ev2.JobQueueChange = new JobQueueChangeEvent();
          ev2.JobQueueChange.jobQueueName = "root.hdfs";
          jq.event = ev2;

          JobFinished jf = new JobFinished();
          jf.type = "JOB_FINISHED";
          JobFinished.Event ev3 = new JobFinished.Event();
          ev3.JobFinished = new JobFinishedEvent();
          ev3.JobFinished.finishTime = endMs;
          jf.event = ev3;

          try {
            String submitStr = mapper.writeValueAsString(js);
            submitStr = submitStr.replace("JobSubmitted", "org.apache.hadoop.mapreduce.jobhistory.JobSubmitted");
            //System.out.println(submitStr);

            String queueStr = mapper.writeValueAsString(jq);
            queueStr = queueStr.replace("JobQueueChange", "org.apache.hadoop.mapreduce.jobhistory.JobQueueChange");
            //System.out.println(queueStr);

            String finishStr = mapper.writeValueAsString(jf);
            finishStr = finishStr.replace("JobFinished", "org.apache.hadoop.mapreduce.jobhistory.JobFinished");
            //System.out.println(finishStr);

            FileUtils.writeStringToFile(new File(dir, jhistFileName),
                Joiner.on('\n').join(submitStr, queueStr, finishStr));
          } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
          }

          jobCounter++;
          csvWriter.writeNext(new String[] {
              oe.identity,
              escapeQueryText(queryText),
              Long.toString(kv.getKey()),
              Long.toString(kv.getValue())
          });
        }
      }
//      csvWriter.writeNext(new String[] {Integer.toString(index++),
//          identity,
//          Integer.toString(totalExecutions),
//          Long.toString(minDuration),Long.toString(maxDuration),
//          Long.toString(averageDuration),Long.toString(medianDuration),
//          lastPrincipal, lastStarted != null ? lastStarted.toString() : "", queryType, queryText});

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
    String entityQuery = "sourceType:HIVE AND (type:operation_execution) and started:[2016-04-06T06:59:00.000Z TO 2016-05-06T06:59:00.000Z]";

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

    // Collect operation details like queryText, etc and write it out.
    CSVWriter csvWriter =
        new CSVWriter(outputWriter, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.DEFAULT_QUOTE_CHARACTER, "\n");


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
//    try {
//      String markerWritePath = args[3];
//      PrintWriter markerWriter = new PrintWriter(markerWritePath, "UTF-8");
//      markerWriter.println(nextMarker);
//      markerWriter.close();
//    } catch(IOException e) {
//      throw Throwables.propagate(e);
//    } catch (ArrayIndexOutOfBoundsException e){
//      LOG.error("Please specify a file to save next marker");
//    }
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