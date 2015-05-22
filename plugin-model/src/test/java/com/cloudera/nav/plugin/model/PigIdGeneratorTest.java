package com.cloudera.nav.plugin.model;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Maps;

import java.util.Map;

import org.apache.commons.configuration.MapConfiguration;
import org.junit.*;

public class PigIdGeneratorTest {

  @Before
  public void testOperationId() {
    String jobName = "Brain";
    String plan = "The same thing we do every night pinky";
    Map<String, Object> map = Maps.newHashMap();
    map.put(PigIdGenerator.PIG_LOGICAL_PLAN_HASH_PROP, plan);
    MapConfiguration jobConf = new MapConfiguration(map);
    String id = PigIdGenerator.generateNewOperationId(jobName, jobConf);
    assertEquals(MD5IdGenerator.generateIdentity(jobName, plan), id);
  }

  @Before
  public void testExecId() {
    Map<String, Object> map = Maps.newHashMap();
    String script = "acme.plan.no.2012374587";
    map.put(PigIdGenerator.PIG_SCRIPT_ID_PROP, script);
    MapConfiguration jobConf = new MapConfiguration(map);
    String id = PigIdGenerator.generateExecutionId(jobConf);
    assertEquals(MD5IdGenerator.generateIdentity(script), id);
  }
}
