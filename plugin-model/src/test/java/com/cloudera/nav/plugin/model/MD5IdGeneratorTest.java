package com.cloudera.nav.plugin.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.*;

public class MD5IdGeneratorTest {

  @Test
  public void testBasic() {
    String hash = MD5IdGenerator.generateIdentity("foo");
    assertEquals(MD5IdGenerator.generateIdentity("foo"), hash);
    assertNotEquals(MD5IdGenerator.generateIdentity("bar"), hash);
    assertEquals(hash.length(), 32);
  }
}
