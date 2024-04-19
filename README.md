Goals:

### self-service and schema based
users should be able to add and update new entities with little or no help from the maintainers

users should be able to declare data structure 
- mark certain fields as sensitive and have the library automatically redact them
- list a resource per parent (provider)
- list all resources of a type
- filter resources by a field (the field has to be mapped as indexed in the schema to improve performance)


### audit log
all writes to the database should be event based - we should be able to see who did what and when

todo:think about audit columns

### 

todo

### type-safe
