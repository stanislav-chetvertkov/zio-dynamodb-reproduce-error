
### Requirements

- ability to store multiple types of data in a single column
- an instance of a resource can either be global or belong to a parent resource
- Optimistic locking - when updating a resource instance by id we have to provider the target version for the entry
  - if the version in the database is different from the one provided by the client the update will fail
- ability to store history of changes per resource instance
  - has to be non-invasive from the performance point of view
    - querying for history should be quick (based on indexes)
  - todo: ability to rollback to a previous version of the resource
- self-service - users could add new types of resources with writing minimal amount of code


## Design

### Multi table approach in DynamoDB

In dynamoDB is common to use a single table to store multiple types of data.
Usually it is done by using composite sorting keys and global secondary indexes.

sorting keys are sorted in lexicographical order 
so by using a scheme like 

`resource_type#resource_id`

So for example, lets say we have an organization 'org1' that has multiple addresses and multiple contact persons

we could store it in the following way

```
pk -> org1
sk -> address#1
...
address_column1 -> value1
address_column2 -> value2
...
```

and 

```
pk -> org1
sk -> contact_person#id_of_the_contact_person
...
contact_person_column1 -> value1
contact_person_column2 -> value2
...
```

This way of modeling allows us to have access patterns like:
- query all addresses for the organization
- query all resources (addresses and contact persons) for the organization


#### Additional access patterns

We still cannot do a query to get a resource by id without knowing parent - organization in this case which is
represented by the partition key.

We could do a full table scan and filter the results by id but it is not efficient and comes with a cost.

We could use a global secondary index to achieve that.

a few notes about global secondary indexes:
- Global secondary indexes have a slight delay due to the data being replicated 
- GSIs are read only


So we introduce a new GSI for the table, currently called `gsi1` but the name is arbitrary and just need to be consistent.
it has a partition key and a sorting key called `GSI_PK` and `GSI_SK` respectively.

So we modify our insert to also write to the main table we also add values for `GSI_PK` and `GSI_SK`

Initially I set it to
```
"gsi_pk1" -> resourcePrefix
"gsi_sk1" -> resource_id
```

It allowed to list all entries of a particular resource type, i.e. list all contact persons or list all the available addresses

Getting a particular instance is also easy, one can
GetItem with hash key equals to, for example, 'contact_person' and sk begins with 

An assumption that IDs have to be of the same length because sorting keys are alphanumerically ordered and one needs to
separate cases such as '1', '10', '11', '110' etc,  otherwise begins with will match a potentially wrong item

using UUID for the ids could solve the problem but if the ID has to mean something like, person's government id, zip code etc. 
this approach is not going to work. To mitigate that one could use 'resource_id#' where # is a separator to mark the end of the id


### ZIO schema

ZIO schema is a great alternative to macros or code generation.
It should have similar performance characteristics to those because the derivation is done at compile time.

So I to model a particular type of resource I've decided to use simple case classes with some custom annotations
to mark certain fields

here is an example of such a declaration

```scala
  @resource_prefix("voice_endpoint")
  case class VoiceEndpoint(@id_field voice_id: String,
                         ip: String,
                         capacity: Int,
                         @parent_field parent: String) // will keep it as a string for now
```

### Validation



### History

In order to be able to save history of changes and still have the ability of transactional updates based on optimistic locking
every time we update a resource we will write 2 records to the database:

```scala
  case class VoiceEndpoint(@id_field voice_id: String,
                           ip: String,
                           capacity: Int,
                           @parent_field parent: String)
```

we get 2 records

```
 VoiceEndpoint(voice_id = "voice1", ip = "127.0.0.1", capacity = 10, parent = "provider#3")
 
 pk -> provider#3
 sk -> voice_endpoint#voice1#1
 GSI_PK -> voice1
 GSI_SK -> 1
 

 
```

- the new version of the resource

- history record
    history records are read only and are never updated



## TODOS:

- [ ] deletions
- [ ] case classes with complex fields 
- [ ] validation when inserting data
- [ ] auto migrations when changing the schema
- [ ] bulk insert
- [ ] bulk update per parent
- [ ] integration test with exposed REST API