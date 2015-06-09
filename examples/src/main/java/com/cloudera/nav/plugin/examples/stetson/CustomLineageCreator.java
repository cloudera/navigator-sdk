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

  public static void main(String[] args) {
    CustomLineageCreator lineageCreator = new CustomLineageCreator(args[0]);
    lineageCreator.run();
  }

  protected final NavigatorPlugin plugin;

  public CustomLineageCreator(String configFilePath) {
    this.plugin = NavigatorPlugin.fromConfigFile(configFilePath);
  }

  public void run() {
    // Create the template
    StetsonScript script = createStetsonScript();
    // Create the instance
    StetsonExecution exec = createExecution();
    // Connect the template and instance
    exec.setTemplate(script);
    // Write metadata
    plugin.write(exec);
  }

  protected StetsonScript createStetsonScript() {
    StetsonScript script = new StetsonScript(plugin.getNamespace());

    // Change according to actual operation. Use the PigIdGenerator to generate
    // the correct identities based on the job name and the pig.logicalPlan.hash
    // from the job conf
    String pigOperationId = "d232bb9146edace98f5fbddfb05e5ef0";
    script.setPigOperation(pigOperationId);
    script.setIdentity(script.generateId());

    script.setName("Stetson Script");
    script.setOwner("Chang");
    script.setScript("LOAD\nGROUPBY\nAGGREGATE");
    script.setDescription("I am a custom operation template");
    return script;
  }

  protected StetsonExecution createExecution() {
    StetsonExecution exec = new StetsonExecution(plugin.getNamespace());

    // Change according to actual execution. Use the PigIdGenerator to generate
    // the correct identities based on the job name and the pig.script.id
    // from the job conf
    String pigExecutionId = "f3603812e2c4d95e7e6bbc9afbabc160";
    exec.setPigExecution(pigExecutionId);

    exec.setName("Stetson Execution");
    exec.setDescription("I am a custom operation instance");
    exec.setLink("http://hasthelargehadroncolliderdestroyedtheworldyet.com/");
    exec.setStarted(Instant.now());
    exec.setEnded((new Instant(Instant.now().toDate().getTime() + 10000)));
    return exec;
  }
}
