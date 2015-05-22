package com.cloudera.nav.plugin.model.relations;

import static org.junit.Assert.assertEquals;

import org.junit.*;

public class RelationTypeTest {

  @Test
  public void testDataFlow() {
    assertEquals(RelationType.DATA_FLOW.getEndpoint1Role(),
        RelationRole.SOURCE);
    assertEquals(RelationType.DATA_FLOW.getEndpoint2Role(),
        RelationRole.TARGET);
  }

  @Test
  public void testParentChild() {
    assertEquals(RelationType.PARENT_CHILD.getEndpoint1Role(),
        RelationRole.PARENT);
    assertEquals(RelationType.PARENT_CHILD.getEndpoint2Role(),
        RelationRole.CHILD);
  }

  @Test
  public void testLogicalPhysical() {
    assertEquals(RelationType.LOGICAL_PHYSICAL.getEndpoint1Role(),
        RelationRole.LOGICAL);
    assertEquals(RelationType.LOGICAL_PHYSICAL.getEndpoint2Role(),
        RelationRole.PHYSICAL);
  }

  @Test
  public void testInstanceOf() {
    assertEquals(RelationType.INSTANCE_OF.getEndpoint1Role(),
        RelationRole.TEMPLATE);
    assertEquals(RelationType.INSTANCE_OF.getEndpoint2Role(),
        RelationRole.INSTANCE);
  }
}
