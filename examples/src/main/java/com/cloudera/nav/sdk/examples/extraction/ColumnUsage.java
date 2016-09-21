package com.cloudera.nav.sdk.examples.extraction;

import com.cloudera.nav.sdk.client.ClientConfig;
import com.cloudera.nav.sdk.client.ClientConfigFactory;
import com.cloudera.nav.sdk.client.MetadataExtractor;
import com.cloudera.nav.sdk.client.MetadataResultIterator;
import com.cloudera.nav.sdk.client.MetadataResultSet;
import com.cloudera.nav.sdk.client.NavApiCient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.opencsv.CSVWriter;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The program extracts columns for a given table in a database and
 * prints the usage of these columns. It goes through all the lineage operaitons
 * that are triggered from these columns and counts them.
 *
 * This program can be further enhanced in the future to get all the users and
 * other statistics as well.
 *
 * Program Arguments:
 *  configFile outputFile databaseName columnName
 */
public class ColumnUsage {
  private static final Logger LOG =
      LoggerFactory.getLogger(ColumnUsage.class);

  private static Integer limit = 500000;
  private static final ObjectMapper mapper= new ObjectMapper();

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws IOException {
    LogManager.getRootLogger().setLevel(Level.INFO);

    String configFilePath = args[0];
    ClientConfig config = (new ClientConfigFactory())
        .readConfigurations(configFilePath);

    extractTable(config, args[1], args[2], args[3]);
  }

  static class Column implements Comparable<Object> {
    String identity;
    String name;

    Set<String> destinationQueryParts = Sets.newHashSet();
    Set<String> destinationOperations = Sets.newHashSet();
    Set<String> destinationOperationInstances = Sets.newHashSet();
    Set<String> users = Sets.newHashSet();

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Column column = (Column) o;

      if (identity != null ? !identity.equals(column.identity) : column
          .identity != null)
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      return identity != null ? identity.hashCode() : 0;
    }

    @Override
    public int compareTo(Object o) {
      if (o instanceof String) {
        return identity.compareTo(identity);
      }
      return identity.compareTo(((Column)o).identity);
    }
  }

  private static void extractTable(ClientConfig config, String file,
                                   String databaseName, String tableName) throws IOException {
    NavApiCient client = new NavApiCient(config);
    MetadataExtractor extractor = new MetadataExtractor(client, limit);

    Map<String, Column> columns = Maps.newHashMap();

    // Collect all the partition columns
    String entityQuery =
        "+parentPath:\\/" + databaseName + "\\/" + tableName;
    MetadataResultSet resultSet = extractor.extractMetadata(null, null,
        entityQuery, null);
    MetadataResultIterator iterator = resultSet.getEntities().iterator();

    while(iterator.hasNext()) {
      Map<String, Object> obj = iterator.next();
      Column column = new Column();
      column.identity = (String) obj.get("identity");
      column.name = (String) obj.get("originalName");

      columns.put(column.identity, column);
    }
    LOG.info("Total processed {} columns", columns.size());

    // Now, lets get all the relations from this column which are not parentChild.
    String entityIds = Joiner.on(",").join(columns.keySet());
    String relationsQuery =
        "{!terms f=endpoint1Ids}" + entityIds + ",dummy";

    //Map<String, Column> columns = Maps.newHashMap();
    Map<String, Set<String>> queryPartToColumn = Maps.newHashMap();

    // Got all the columns...Go through the end point 2 Ids which are query parts
    // and from there, get the operation and executions.
    resultSet = extractor.extractMetadata(null, null, null, relationsQuery);
    iterator = resultSet.getRelations().iterator();
    while(iterator.hasNext()) {
      Map<String, Object> obj = iterator.next();

      @SuppressWarnings("unchecked")
      Map<String, Object> sources = (Map<String, Object>) obj.get("sources");
      List<String> sourceIds = (List<String>) sources.get("entityIds");

      @SuppressWarnings("unchecked")
      Map<String, Object> destinations = (Map<String, Object>) obj.get("targets");
      List<String> destinationIds = (List<String>) destinations.get("entityIds");

      for(String destinationId : destinationIds) {
        Set<String> columnIds = queryPartToColumn.get(destinationId);
        if (columnIds == null) {
          columnIds = Sets.newHashSet();
          queryPartToColumn.put(destinationId, columnIds);
        }
        columnIds.addAll(sourceIds);
      }
    }

    // Go through the destinationIds and get the parent's for them, which would be
    // operations.
    entityIds = Joiner.on(",").join(queryPartToColumn.keySet());
    relationsQuery =
        "type:PARENT_CHILD AND {!terms f=endpoint2Ids}" + entityIds + ",dummy";

    Map<String, Set<String>> queryToQueryParts = Maps.newHashMap();

    resultSet = extractor.extractMetadata(null, null, null, relationsQuery);
    iterator = resultSet.getRelations().iterator();
    while(iterator.hasNext()) {
      Map<String, Object> obj = iterator.next();

      @SuppressWarnings("unchecked")
      Map<String, Object> parent = (Map<String, Object>) obj.get("parent");
      String parentId = (String) parent.get("entityId");

      Set<String> queryParts = queryToQueryParts.get(parentId);
      if (queryParts == null) {
        queryParts = Sets.newHashSet();
        queryToQueryParts.put(parentId, queryParts);
      }


      @SuppressWarnings("unchecked")
      Map<String, Object> children = (Map<String, Object>) obj.get("children");
      List<String> childrenIds = (List<String>) children.get("entityIds");
      queryParts.addAll(childrenIds);
    }

    // Go through the destinationIds and get the parent's for them, which would be
    // operations.
    entityIds = Joiner.on(",").join(queryToQueryParts.keySet());
    relationsQuery =
        "type:INSTANCE_OF AND {!terms f=endpoint1Ids}" + entityIds + ",dummy";

    resultSet = extractor.extractMetadata(null, null, null, relationsQuery);
    iterator = resultSet.getRelations().iterator();
    while(iterator.hasNext()) {
      Map<String, Object> obj = iterator.next();

      @SuppressWarnings("unchecked")
      Map<String, Object> template = (Map<String, Object>) obj.get("template");
      String templateId = (String) template.get("entityId");

      @SuppressWarnings("unchecked")
      Map<String, Object> instances = (Map<String, Object>) obj.get("instances");
      List<String> instanceIds = (List<String>) instances.get("entityIds");

      Set<String> queryParts = queryToQueryParts.get(templateId);
      for(String queryPart : queryParts) {
        Set<String> columnIds = queryPartToColumn.get(queryPart);
        for(String columnId : columnIds) {
          Column column = columns.get(columnId);
          column.destinationOperations.add(templateId);
          column.destinationOperationInstances.addAll(instanceIds);
        }
      }
    }

    PrintWriter writer = new PrintWriter(file, "UTF-8");

    CSVWriter csvWriter = new CSVWriter(writer);
    LOG.info("Usage for: {}/{}", databaseName, tableName);
    csvWriter.writeNext(new String[] {"ColumnName, Unique Queries, Total Executions"});

    for(Map.Entry<String, Column> entry : columns.entrySet()) {
      Column column = entry.getValue();
      csvWriter.writeNext(new String[]{
          column.name,
          String.valueOf(column.destinationOperations.size()),
          String.valueOf(column.destinationOperationInstances.size())});
    }
    csvWriter.close();
  }
}