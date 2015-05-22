package com.cloudera.nav.plugin.model.relations;

import static org.junit.Assert.*;

import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.Iterator;

import org.junit.*;

public class RelationRoleTest {

  private Collection<RelationRole> ep1Roles;
  private Collection<RelationRole> ep2Roles;

  @Before
  public void setUp() {
    ep1Roles = ImmutableList.of(
        RelationRole.PARENT, RelationRole.LOGICAL,
        RelationRole.SOURCE, RelationRole.TEMPLATE);
    ep2Roles = ImmutableList.of(
        RelationRole.CHILD, RelationRole.PHYSICAL,
        RelationRole.TARGET, RelationRole.INSTANCE);
  }

  @Test
  public void testEndPoint() {
    for (RelationRole ep1 : ep1Roles) {
      assertEquals(ep1.getEndPoint(), RelationRole.EndPoint.ENDPOINT1);
    }
    for (RelationRole ep2 : ep2Roles) {
      assertEquals(ep2.getEndPoint(), RelationRole.EndPoint.ENDPOINT2);
    }
  }

  @Test
  public void testInverseRole() {
    Iterator<RelationRole> $inverse = ep2Roles.iterator();
    for (RelationRole ep1 : ep1Roles) {
      assertEquals(ep1.getInverseRole(), $inverse.next());
    }
  }
}
