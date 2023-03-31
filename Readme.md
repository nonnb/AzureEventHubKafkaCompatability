#Azure Event Hubs - Kafka compatability Demo#

The following opinions are taken:

 - Each producer produces 10 consecutively numbered messages to single topic. Depending on the partitioning on the topic, messages will be round-robined between the partitions unless a partition key is specified.
  
- Kafka will automatically create topics and entries for Consumer Group offsets automatically, however Event Hubs will not - you will need to manually add the Topic and Consumer Groups through the Portal blade.
    - Note that Kafka is case-sensitive. In Azure, almost all integration and storage names should be [lower cased](https://docs.microsoft.com/en-us/azure/architecture/best-practices/naming-conventions#storage)
 
Kafka stores the offsets on an internal Topic (Event Hubs used to store Offsets on a Separate Blob Storage account)
  
Whenever partitions are assigned or revoked by the broker, consumers write out their new assignments.
