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

package com.cloudera.nav.plugin.client.examples.stetson2;

import com.cloudera.nav.plugin.client.NavApiCient;
import com.cloudera.nav.plugin.client.NavigatorPlugin;
import com.cloudera.nav.plugin.client.PluginConfigurationFactory;
import com.cloudera.nav.plugin.client.PluginConfigurations;
import com.cloudera.nav.plugin.model.MD5IdGenerator;
import com.cloudera.nav.plugin.model.Source;
import com.cloudera.nav.plugin.model.SourceType;

import org.joda.time.Instant;

/**
 * In this example we show a more complex example of how to create custom entity
 * types and how to link them to hadoop entities. For a description of the base
 * example please see {@link com.cloudera.nav.plugin.client.examples.stetson.AddLineage}.
 *
 * As an extension of the previous example, we allow the user to specify custom
 * input and output datasets to a StetsonExecution. We define a new custom
 * entity called StetsonDataset. This is a logical dataset in the Stetson
 * application and is physically represented by an HDFS directory.
 *
 */
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
    String inputName = "StetsonInput"; // Stetson's name for the input dataset
    String outputName = "StetsonOutput"; // Stetson's name for the output data
    String inputPath = "/dualcore/web_logs"; // path of HDFS dir for input dataset
    String outputPath = "/dualcore/order_details"; // path of HDFS dir for output dataset

    // Create the template
    StetsonScript script = new StetsonScript(config.getNamespace());
    script.setScript("LOAD\nGROUPBY\nAGGREGATE");
    script.setName("myScript");
    script.setOwner("Chang");
    script.setPigOperation(operationId);
    script.setIdentity(script.generateId());
    script.setDescription("I am a custom operation template");

    // Create the instance
    NavApiCient client = new NavApiCient(config);
    Source hdfs = client.getOnlySource(SourceType.HDFS);
    String inputHdfsId = MD5IdGenerator.generateIdentity(hdfs.getIdentity(),
        inputPath);
    String outputHdfsId = MD5IdGenerator.generateIdentity(hdfs.getIdentity(),
        outputPath);

    StetsonExecution exec = new StetsonExecution(config.getNamespace());
    exec.setName("myExecution");
    exec.setTemplate(script);
    exec.setPigExecution(execId);
    exec.addInput(inputName, inputHdfsId);
    exec.addOutput(outputName, outputHdfsId);
    exec.setStarted(Instant.now());
    exec.setEnded((new Instant(Instant.now().toDate().getTime() + 10000)));

    exec.setDescription("I am a custom operation instance");
    exec.setLink("http://hasthelargehadroncolliderdestroyedtheworldyet.com/");

    plugin.write(exec);
  }
}
