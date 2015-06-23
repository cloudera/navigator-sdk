/*
 * Copyright (c) 2015 Cloudera, Inc.
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

package com.cloudera.nav.plugin.examples.stetson;

import com.cloudera.nav.plugin.client.NavigatorPlugin;

import org.joda.time.Instant;

/**
 * In this example we show how to create custom entity types and
 * how to link them to hadoop entities. We define a custom operation entity
 * called StetsonScript and a custom operation execution entity called
 * StetsonExecution. A StetsonScript is the template for a StetsonExecution
 * and so we use the @MRelation annotation to specify an InstanceOf relationship
 * between the StetsonExecution and StetsonScript.
 *
 * StetsonScript and StetsonExecution are logical entities that exist in a
 * hypothetical Stetson application. In Hadoop, these operations are carried out
 * using Pig. In order to establish the relationship between the Stetson
 * custom entities and the hadoop entities, we again use the @MRelation
 * annotation to form LogicalPhysical relationships.
 *
 * Relations created:
 *
 * StetsonScript ---(LogicalPhysical)---> Pig Operation
 * StetsonExecution ---(LogicalPhysical)---> Pig Execution
 *
 * The relationships between Pig operation and execution are created
 * automatically by Navigator
 */
public class CustomLineageCreator {

  /**
   * @param args 1. config file path
   *             2. Pig operation id
   *             3. Pig execution id
   */
  public static void main(String[] args) {
    CustomLineageCreator lineageCreator = new CustomLineageCreator(args[0]);
    lineageCreator.setPigOperationId(args[1]);
    lineageCreator.setPigExecutionId(args[2]);
    lineageCreator.run();
  }

  protected final NavigatorPlugin plugin;
  private String pigOperationId;
  private String pigExecutionId;

  public CustomLineageCreator(String configFilePath) {
    this.plugin = NavigatorPlugin.fromConfigFile(configFilePath);
  }

  public void run() {
    // Create the template
    StetsonScript script = createStetsonScript();
    // Create the instance
    StetsonExecution exec = createExecution();
    // Connect the template and instance
    script.setIdentity(script.generateId());
    exec.setTemplate(script);
    // Write metadata
    plugin.write(exec);
  }

  public String getPigOperationId() {
    return pigOperationId;
  }

  public String getPigExecutionId() {
    return pigExecutionId;
  }

  public void setPigOperationId(String pigOperationId) {
    this.pigOperationId = pigOperationId;
  }

  public void setPigExecutionId(String pigExecutionId) {
    this.pigExecutionId = pigExecutionId;
  }

  protected StetsonScript createStetsonScript() {
    StetsonScript script = new StetsonScript(plugin.getNamespace());
    script.setPigOperation(getPigOperationId());
    script.setName("Stetson Script");
    script.setOwner("Chang");
    script.setScript("LOAD\nGROUPBY\nAGGREGATE");
    script.setDescription("I am a custom operation template");
    return script;
  }

  protected StetsonExecution createExecution() {
    StetsonExecution exec = new StetsonExecution(plugin.getNamespace());
    exec.setPigExecution(getPigExecutionId());
    exec.setName("Stetson Execution");
    exec.setDescription("I am a custom operation instance");
    exec.setLink("http://hasthelargehadroncolliderdestroyedtheworldyet.com/");
    exec.setStarted(Instant.now());
    exec.setEnded((new Instant(Instant.now().toDate().getTime() + 10000)));
    return exec;
  }
}
