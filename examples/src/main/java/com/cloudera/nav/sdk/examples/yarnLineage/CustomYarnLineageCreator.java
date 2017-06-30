
/*
 * Copyright (c) 2017 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.nav.sdk.examples.yarnLineage;

import com.cloudera.nav.sdk.client.NavApiCient;
import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.client.writer.ResultSet;
import com.cloudera.nav.sdk.model.MRIdGenerator;
import com.cloudera.nav.sdk.model.Source;
import com.cloudera.nav.sdk.model.SourceType;

import org.joda.time.Instant;

/**
 * In this example we show how to create custom entity types and
 * how to link them to hadoop entities. We define a custom operation entity
 * from a hypothetical application called Stetson. The Stetson application
 * defines a custom operations called StetsonScript and a custom operation
 * execution entity called StetsonExecution. A StetsonScript is the template
 * for a StetsonExecution and so we use the @MRelation annotation to specify an
 * InstanceOf relationship between the StetsonExecution and StetsonScript.
 * <p>
 * In Hadoop, Stetson operations are carried out using Yarn. In order to
 * establish the relationship between the Stetson custom entities and the
 * hadoop entities, we again use the @MRelation annotation to form
 * LogicalPhysical relationships.
 * <p>
 * Relations created:
 * <p>
 * StetsonScript ---(LogicalPhysical)---> Yarn Operation
 * StetsonExecution ---(LogicalPhysical)---> Yarn Execution
 * `
 * The relationships between Yarn operation and execution are created
 * automatically by Navigator
 */

public class CustomYarnLineageCreator {
  /**
   * @param args 1. config file path
   *             2. The YARN Operation Job Name
   *             3. The YARN Execution job Id
   *             4. The mapper used by the YARN job
   *             5. The reducer used by the YARN job
   */
  public static void main(String[] args) {
    CustomYarnLineageCreator lineageCreator = new CustomYarnLineageCreator(args[0]);
    lineageCreator.setYarnJobName(args[1]);
    lineageCreator.setYarnExecutionJobId(args[2]);
    lineageCreator.setMapper("mapper");
    lineageCreator.setReducer("reducer");
    lineageCreator.run();
  }

  protected final NavigatorPlugin plugin;
  private String yarnJobName;
  private String mapper;
  private String reducer;
  private String yarnExecutionJobId;
  private String yarnOperationId;
  private String yarnExecutionId;
  private String sourceId;

  public CustomYarnLineageCreator(String configFilePath) {
    this.plugin = NavigatorPlugin.fromConfigFile(configFilePath);
  }

  public void run() {
    // register all models in example
    plugin.registerModels(getClass().getPackage().getName());

    NavApiCient client = plugin.getClient();
    Source fs = client.getSourcesForType(SourceType.YARN).iterator().next();

    // We need to set the Source Id here which is used in the generation of
    // YARN operation and execution ID's by the MR1dGenerator.
    setSourceId(fs.getIdentity());
    setYarnOperationId();
    setYarnExecutionId();

    StetsonScript script = createStetsonScript();
    // Create the instance
    StetsonExecution exec = createExecution();
    // Connect the template and instance
    script.setIdentity(script.generateId());
    exec.setTemplate(script);
    // Write metadata
    ResultSet results = plugin.write(exec);

    if (results.hasErrors()) {
      throw new RuntimeException(results.toString());
    }
  }

  public void setYarnJobName(String yarnJobName) {
    this.yarnJobName = yarnJobName;
  }

  public String getYarnJobName() { return yarnJobName;}

  public void setYarnExecutionJobId(String yarnExecutionJobIdId) {
    this.yarnExecutionJobId = yarnExecutionJobIdId;
  }

  public String getYarnExecutionJobId() { return yarnExecutionJobId;}

  public void setMapper(String mapper) {
    this.mapper = mapper;
  }

  public String getMapper() { return mapper;}

  public void setReducer(String reducer) {
    this.reducer = reducer;
  }

  public String getReducer() { return reducer;}

  public void setYarnExecutionId() {
    yarnExecutionId = MRIdGenerator.generateJobExecIdentity(
        getSourceId(), getYarnExecutionJobId());
  }

  public void setYarnOperationId() {
    yarnOperationId = MRIdGenerator.generateJobIdentity(
        getSourceId(), getYarnJobName(), getMapper(), getReducer());
  }

  public String getYarnExecutionId() { return yarnExecutionId;}

  public String getYarnOperationId() { return yarnOperationId;}

  public void setSourceId(String sourceId) {
    this.sourceId = sourceId;
  }

  public String getSourceId() {
    return sourceId;
  }

  protected StetsonScript createStetsonScript() {
    StetsonScript script = new StetsonScript(plugin.getNamespace());
    script.setOperation(getYarnOperationId(), SourceType.YARN);
    script.setName("Stetson Script");
    script.setOwner("Chang");
    script.setDescription("I am a custom operation template");
    return script;
  }

  protected StetsonExecution createExecution() {
    StetsonExecution exec = new StetsonExecution(plugin.getNamespace());
    exec.setExecution(getYarnExecutionId(), SourceType.YARN);
    exec.setName("Stetson Execution");
    exec.setDescription("I am a custom operation instance");
    exec.setLink("http://hasthelargehadroncolliderdestroyedtheworldyet.com/");
    exec.setIndex(10);
    exec.setSteward("chang");
    exec.setStarted(Instant.now());
    exec.setEnded((new Instant(Instant.now().toDate().getTime() + 10000)));
    return exec;
  }
}
