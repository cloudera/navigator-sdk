package com.cloudera.nav.sdk.examples.extraction;


import com.google.common.base.Throwables;

import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Create QueryExtractionConfig instance
 */
public class QueryExtractionConfigFactory {
  public static final String OUTPUT_DIRECTORY = "output_directory";
  public static final String PRINCIPAL = "principal";
  public static final String START_TIME = "start_time";
  public static final String END_TIME = "end_time";
  public static final String DURATION = "duration";
  public static final String OPERATION_EXECUTION_QUERY = "operation_execution_query";
  public static final String OPTIMIZER_DELIMITER = "optimizer_delimiter";

  public QueryExtractionConfig readConfigurations(String filePath) {
    try {
      PropertiesConfiguration props = new PropertiesConfiguration(filePath);
      QueryExtractionConfig config = new QueryExtractionConfig();
      config.setOutputDirectory(props.getString(OUTPUT_DIRECTORY));
      config.setPrincipal(props.getString(PRINCIPAL));
      config.setStartTime(props.getString(START_TIME));
      config.setEndTime(props.getString(END_TIME));
      config.setDuration(props.getString(DURATION));
      config.setOperationExecutionQuery(props.getString(OPERATION_EXECUTION_QUERY));
      config.setOptimizerDelimiter(props.getString(OPTIMIZER_DELIMITER, "\n"));
      return config;
    } catch (ConfigurationException e) {
      throw Throwables.propagate(e);
    }
  }

  public QueryExtractionConfig fromConfigMap(Map<String, Object> props) {
    QueryExtractionConfig config = new QueryExtractionConfig();
    config.setOutputDirectory(props.get(OUTPUT_DIRECTORY).toString());
    config.setPrincipal(props.containsKey(PRINCIPAL) ?
        props.get(PRINCIPAL).toString() : null);
    config.setStartTime(props.get(START_TIME).toString());
    config.setEndTime(props.get(END_TIME).toString());
    config.setDuration(props.containsKey(DURATION) ?
        props.get(DURATION).toString() : "monthly");
    config.setOperationExecutionQuery(props.containsKey(OPERATION_EXECUTION_QUERY) ?
        props.get(OPERATION_EXECUTION_QUERY).toString() : null);
    config.setOptimizerDelimiter(props.containsKey(OPTIMIZER_DELIMITER) ?
        props.get(OPTIMIZER_DELIMITER).toString() : "\n");
    return config;
  }
}
