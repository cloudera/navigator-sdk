package com.cloudera.nav.plugin.model;

import static org.junit.Assert.assertEquals;

import org.junit.*;

public class SourceTest {

  @Test
  public void testBasic() {
    String sourceName = "mySource";
    String clusterName = "myCluster";
    Source src = new Source(sourceName, SourceType.NONE,
        clusterName, "http://host:port");
    assertEquals(MD5IdGenerator.generateIdentity(clusterName, sourceName),
        src.getIdentity());
    assertEquals(src, new Source(sourceName, SourceType.HDFS,
        clusterName, "http://newHost:newPort"));
  }
}
