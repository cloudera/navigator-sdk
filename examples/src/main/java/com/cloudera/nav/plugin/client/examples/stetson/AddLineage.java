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

package com.cloudera.nav.plugin.client.examples.stetson;

import com.cloudera.nav.plugin.client.NavigatorPlugin;
import com.cloudera.nav.plugin.client.PluginConfigurationFactory;
import com.cloudera.nav.plugin.client.PluginConfigurations;

import org.joda.time.Instant;

public class AddLineage {

  public static void main(String[] args) {
    // setup the plugin and api client
    String configFilePath = args[0];
    PluginConfigurations config = (new PluginConfigurationFactory())
        .readConfigurations(configFilePath);
    NavigatorPlugin plugin = new NavigatorPlugin(config);

    // change according to actual operation and executions
    // you can use the PigIdGenerator to generate the correct
    // identities based on the job name and job conf
    String operationId = "41d5c5382aa0a15f64522dd700bb5765";
    String execId = "96e1a2bebec347c8b8009b1025294a6c";

    // Create the template
    StetsonScript script = new StetsonScript();
    script.setNamespace(config.getNamespace());
    script.setScript("LOAD\nGROUPBY\nAGGREGATE");
    script.setName("myScript");
    script.setOwner("Chang");
    script.setPigOperationId(operationId);
    script.setIdentity(script.generateId());
    script.setDescription("I am a custom operation template");

    // Create the instance
    StetsonExecution exec = new StetsonExecution();
    exec.setNamespace(config.getNamespace());
    exec.setName("myExecution");
    exec.setTemplate(script);
    exec.setStarted(Instant.now());
    exec.setEnded((new Instant(Instant.now().toDate().getTime() + 10000)));
    exec.setStetsonInstId("foobarbaz");
    exec.setPigExecutionId(execId);
    exec.setDescription("I am a custom operation instance");
    exec.setLink("http://hasthelargehadroncolliderdestroyedtheworldyet.com/");

    plugin.write(exec);
  }
}
