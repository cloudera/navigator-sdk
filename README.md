Navigator Metadata Client
=========================

A client library used by partner applications to enrich the metadata in
Cloudera Navigator

Partner application users often use high-level DSL to work with data. The DSL
execution is often implemented as MapReduce or Pig jobs. Navigator can capture
the low level operations but has no knowledge about the high-level DSL
representation of the transformations. This document describes how partner
applications can provide this metadata in an open format that’s extracted by
Navigator to augment lineage information. Joint Navigator and partner
application users will be able to see the correspondence between the existing
Navigator lineage and the higher level DSL in the partner application.

As an example, let’s suppose there is a company T which makes a data preparation
application. T allows its users to create a data preparation pipeline using a
DSL called Stetson. The Stetson script is then implemented as a Pig job.
Currently Navigator is only able to capture the underlying Pig job.

However, the user did not specify the work in terms of Pig operations and would
find it much easier to search and analyze provenance in terms of the
Stetson script.


Navigator Metadata
------------------

The metadata managed by Navigator centers around three main concepts - Entity,
Source, and Relation. Entities are objects defined by Hadoop components and
services like HDFS, Hive, MapReduce, Pig, etc. Examples of Entities include
HDFS files and directories, Hive databases, tables, columns, queries, MR jobs,
job instances, and more. For each Hadoop object, Navigator defines a
corresponding Entity subtype.

Each Entity has a Source which is the Hadoop service or component that created
the Entity. Source’s are the deployed service and it has a SourceType which is
the type of service represented. Entities are also associated with an
EntityType, which is the logical type of the object being represented by the
Entity. This could be a file, directory, database, table, column, etc. It is
important to note that the Entity is not associated with any particular Hadoop
component.

For example, there could be an Entity which is of EntityType FILE defined by a
Source of type HDFS reachable at the source url http://<host>:<port>/. Note that
because an Entity is associated with a particular source, we cannot define an
Entity subtype to capture objects from multiple services.

The third major concept is that of a Relation. A Relation defines the
connections between different Entities. A Relation by itself is defined by two
endpoints and is adirectional by itself. Each endpoint must have one or more
Entities that have the same type and source. A Relation is also associated with
a RelationType. Navigator defines several RelationType’s and for each
RelationType, Navigator defines a Relation subclass to make the construction of
these relationships easier. Example RelationType’s include:

DataFlow - the endpoints are sources and targets indicating movement of data
(e.g., query input -> query output)

ParentChild - the endpoints are parent and child(ren) indicating containment
(e.g., directory -> files)

LogicalPhysical - the endpoints are logical and physical indicating the
connection between an abstract entity and its concrete manifestation (e.g.,
Hive table -> HDFS directory which has the data)

InstanceOf - the endpoints are template and instance indicating the connection
between an operation and its execution.



Client Library
--------------

Each time a partner application executes a unit of work, it needs to capture the
set of instructions that was just executed (like the Stetson script) as well as
information about the actual execution (Stetson instance). It will also need to
match the template script and instance with the underlying Hadoop operation and
operation execution (e.g., Pig job and job instance). It needs to provide all of
that information to Navigator along with a commonly agreed upon way to identify
the aforementioned entities.

In order to make this process easier, Navigator will provide a Java client
library to be used by the partner application developer. The library will
provide a generic Entity class that can be extended for the partner applications
templates and instances. Relationships between the templates, instances, and the
underlying Hadoop operations and executions should be specified as part of the
entity model using annotation tools provided by the Navigator client library.

Each instance of Entity is uniquely identified by a string id which is created
by a factory. Depending on the custom entity, the developer may need to create
new entity id factories. Basic validation should be performed when a new Entity
instance is created.

Finally, the client library will provide a NavigatorPlugin class that writes the
new lineage information to a place that Navigator can consume. The
NavigatorPlugin also allows the user to define custom entity types in Navigator.
The plugin interface is specified in Appendix A. Sample cases for the plugin are
provided in the remaining sections of this document.


Custom Entity Types
-------------------

Before the partner application starts writing metadata information, it must
register the metadata model with Navigator. Navigator should be configured to
assign a unique namespace to a particular partner application. While this isn’t
as flexible from the perspective of the partner application, it prevents
namespace collisions if more than one application is providing metadata to
Navigator. Within Navigator, all new custom entity attributes will automatically
be prefixed by the namespace to prevent collisions across plugin users. This
means that, at least for now, users of the Navigator search UI must search by
the full prefixed attribute name.

In order to create custom entities, the Navigator client library provides enums,
annotations, and a plugin to help partner application define and register custom
entities. A sample interface for a custom entity might look like the following:

```
package com.partner.app.lineage.model

@MClass
public class StetsonScript extends Entity {

    @MProperty
    public String getScript();

    @MProperty
    public EntityType getType() {
      return EntityTypes.OPERATION;
    }

    @MRelation(role=RelationRole.PHYSICAL, sourceType=SourceTypes.PIG)
    public String getPigOperationId();
}
```

The client library provides enums used in the above example like EntityType,
RelationType, and RelationRole. Annotations like @MClass, @MProperty, and
@MRelation are also provided and are required for model registration. The client
library also provides an id generator for Pig operations based on the job name
(e.g., “PigLatin:workflow.pig”) and the logical plan hash in the job
configuration.

An instance definition might look like:

```
package com.partner.app.lineage.model

@MClass
public class StetsonInstance extends Entity {

    @MProperty
    public EntityType getType() {
      return EntityTypes.OPERATION_EXECUTION;
    }

    @MProperty
    public Long getStartTime();

    @MRelation(role=RelationRole.TEMPLATE)
    public StetsonScript getTemplate();

    @MRelation(role=RelationRole.PHYSICAL, sourceType=SourceTypes.PIG)
    public String getPigOperationExecutionId();
}
```

The client library will provide an id generator for creating Pig operation
execution id’s based on the operation and the job id given by JobTracker.


Creating Lineage Metadata
-------------------------

The lineage metadata will be written to Navigator. The URI for the location to
which metadata will be written should be provided to the PluginConfigurations.
Currently the plugin only supports a direct push to the Navigator API, so the
URI should look like "http://<host>:<port>/api/v7/plugin". In the future we do
plan to add support for offline writing to HDFS and the local file system.

```
config.setMetadataParentUri("http://<host>:<port>/api/v7/plugin");
```

The steps for creating custom lineage metadata involve creating instances of the
custom entities and using the client library to associate the custom entities
with the underlying Hadoop entities. Let’s use the StetsonScript from above, and
further assume we’ve also defined a StetsonInstance class that corresponds to a
particular execution of a StetsonScript. An example would look like the
following:

```
    // Create the template
    StetsonScript script = new StetsonScript();
    script.setNamespace(config.getNamespace());
    script.setIdentity(script.generateId());
    script.setScript("LOAD\nGROUPBY\nAGGREGATE");
    script.setName("myScript");
    script.setOwner("Chang");
    script.setPigOperationId(operationId);
    script.setDescription("I am a custom operation template");

    // Create the instance
    StetsonExecution exec = new StetsonExecution();
    exec.setNamespace(config.getNamespace());
    exec.setName("myExecution");
    exec.setTemplate(script);
    Date started = new Date();
    exec.setStartTime(started.getTime());
    exec.setEndTime((started.getTime() + 10000));
    exec.setStetsonInstId("foobarbaz");
    exec.setPigExecutionId(execId);
    exec.setDescription("I am a custom operation instance");
```

This completes the lineage augmentation creation process, and the information
can be written for Navigator consumption by simply calling the NavigatorPlugin:

```
plugin.write(exec);
```