
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

    