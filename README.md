Navigator SDK Java Client
=========================

[![Build Status](https://travis-ci.org/cloudera/navigator-sdk.svg?branch=master)](https://travis-ci.org/cloudera/navigator-sdk)

The Cloudera Navigator SDK is a client library that provides functionality to
help users extract metadata from Navigator and to enrich the metadata in
Navigator with custom metadata models, entities, and relationships.

Sample Use Cases
----------------

*Incremental Metadata Extraction*

Certain applications need to extract metadata from Navigator for their own
 purposes or to integrate into an enterprise-wide metadata management system.
 For almost any production Hadoop cluster, it is not feasible to do a full
  extraction of all available metadata every time. Instead, the Navigator SDK
  provides [code examples](examples/src/main/java/com/cloudera/nav/plugin/examples/extraction/MetadataExtraction.java)
  to extract metadata in an incremental fashion.

*Custom Metadata Entities and Lineage Augmentation*

Many ETL and analytics applications built on top of Hadoop use a custom DSL to
work with data. For their enterprise customers, Navigator can serve as the
defacto governance solution. The Navigator SDK provides critical functionality
that allows partner applications to create custom metadata models in Navigator
and link the custom concepts in the external application to the underlying
files, directories, and operations in Hadoop.

As an example, let’s suppose there is a company Foo which makes a data
preparation application. Foo allows its users to create a data preparation
pipeline using a custom DSL. The custom DSL script is then implemented as Pig
jobs. The Navigator SDK would allow Foo to create custom entities in Navigator
to represent the custom DSL operations, executions, and input/output datasets.
It would also allow Foo to then create relationships linking those custom
entities to the underlying Pig jobs and executions. This makes it much easier
for the end user to search and analyze provenance in terms of the custom DSL.

See code examples [here](examples/src/main/java/com/cloudera/nav/plugin/examples/lineage)
and [here](examples/src/main/java/com/cloudera/nav/plugin/examples/lineage2)
 for details.

*Custom Dataset Schema Definition*

Data profiling applications often infer schema from CSV, Json, and other files
that live in Hadoop. In order to help provide a data governance layer for those
 applications, the Navigator SDK can be used to [augment](examples/src/main/java/com/cloudera/nav/plugin/examples/schema)
 the file metadata with the inferred schema's. The schema information can then
 be used to integrate with Navigator's policy engine to perform tasks like
  tagging columns as private information and automatically encrypting sensitive
  data.

*Setting Custom Metadata*

The Navigator API allows users to set tags and custom key-value pairs on
metadata entities. The Navigator SDK provides an easy to use interface for users
 to take advantage of those APIs. The [code examples](examples/src/main/java/com/cloudera/nav/plugin/examples/tags)
 included in the SDK demonstrates setting tags for HDFS and Hive entities.


Navigator Metadata
------------------

The metadata managed by Navigator centers around three main concepts -
[entity](model/src/main/java/com/cloudera/nav/plugin/model/entities/Entity.java),
[source](model/src/main/java/com/cloudera/nav/plugin/model/Source.java),
and [relation](model/src/main/java/com/cloudera/nav/plugin/model/relations/Relation.java).
Entities are objects defined by Hadoop components and
services like HDFS, Hive, MapReduce, Pig, etc. Examples of entities include
HDFS files and directories, Hive databases, tables, columns, queries, MR jobs,
job instances, and more. For each Hadoop object, Navigator defines a
corresponding entity subtype.

Each entity has a source which is the Hadoop service or component that created
the entity. Each source has a source type which is the type of service
represented. For example, Navigator may manage multiple HDFS services with names
 from HDFS-1, HDFS-2, and so on. All of them will have the same source type which
  is HDFS.

Entities are also associated with an entity type, which is the logical type of
the object being represented by the entity. This could be a file, directory,
database, table, column, etc.

The third major concept is that of a relation. A relation defines the
connections between different entities. A relation is defined by two
endpoints and is adirectional by itself. Each endpoint must have one or more
entities that have the same entity type and source. A relation is also
associated with a relation type. Navigator defines several relation types and
for each one, Navigator defines a relation subtype to make the construction of
these relationships easier. The relation types are:

- [DataFlow](model/src/main/java/com/cloudera/nav/plugin/model/relations/DataFlowRelation.java) -
the endpoints are sources and targets indicating movement of data
(e.g., query input -> query output)

- [ParentChild](model/src/main/java/com/cloudera/nav/plugin/model/relations/ParentChildRelation.java) -
the endpoints are parent and child(ren) indicating containment
(e.g., directory -> files)

- [LogicalPhysical](model/src/main/java/com/cloudera/nav/plugin/model/relations/LogicalPhysicalRelation.java) -
the endpoints are logical and physical indicating the
connection between an abstract entity and its concrete manifestation (e.g.,
Hive table -> HDFS directory which has the data)

- [InstanceOf](model/src/main/java/com/cloudera/nav/plugin/model/relations/InstanceOfRelation.java) -
the endpoints are template and instance indicating the connection
between an operation and its execution.



Custom Entities and Relations
-----------------------------

In order to create custom entities, the Navigator client library provides enums,
annotations, and a plugin to help partner application define and register custom
entities. A sample custom entity might look like the following:

```java
@MClass(model = "stetson_op")
public class StetsonScript extends Entity {

  @MRelation(role = RelationRole.PHYSICAL)
  private EndPointProxy pigOperation;
  @MProperty
  private String script;

  public String getScript() {
    return script;
  }

  @Override
  public EntityType getType() {
    return EntityTypes.OPERATION;
  }

  public Entity getPigOperation() {
    return pigOperation;
  }
}
```

The client library provides enums used in the above example like
[EntityType](model/src/main/java/com/cloudera/nav/plugin/model/entities/EntityType.java),
[RelationType](model/src/main/java/com/cloudera/nav/plugin/model/relations/RelationType.java),
and [RelationRole](model/src/main/java/com/cloudera/nav/plugin/model/relations/RelationRole.java).
The [@MClass](model/src/main/java/com/cloudera/nav/plugin/model/annotations/MClass.java)
annotation indicates that the StetsonScript class is a custom metadata model.
The [@MProperty](model/src/main/java/com/cloudera/nav/plugin/model/annotations/MProperty.java)
annotation indicates that script is a model field. The
[@MRelation](model/src/main/java/com/cloudera/nav/plugin/model/annotations/MRelation.java)
annotation in the example indicates that StetsonScript entity should be
connected to the given Pig operation entity with a logical-physical relation
(where the Pig operation is the physical entity).



Writing to Navigator
--------------------

Once the custom metadata models have been defined, we can use the
[NavigatorPlugin](client/src/main/java/com/cloudera/nav/plugin/client/NavigatorPlugin.java)
class to write custom metadata to Navigator. Creating a NavigatorPlugin requires
a set of configurations that includes properties like the Navigator API URL. See
the provided [sample configuration](examples/src/main/resources/sample.conf)
for details.

An example workflow might look like the following:

```java
// Create the plugin
NavigatorPlugin plugin = NavigatorPlugin.fromConfigFile(
  "/home/chang/.navigator/navigator_sdk.conf");

// Create the template
StetsonScript script = new StetsonScript();
script.setNamespace(config.getNamespace());
script.setIdentity(script.generateId());
script.setName("myScript");
script.setPigOperationId(operationId);

// Create the instance
StetsonExecution exec = new StetsonExecution();
exec.setNamespace(config.getNamespace());
exec.setName("myExecution");
exec.setTemplate(script);
exec.setPigExecutionId(execId);

// Write the metadata
plugin.write(exec);
```
