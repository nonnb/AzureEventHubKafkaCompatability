#Azure Event Hubs demo

Similar to the Service Bus demo, this sample proves that a "lowest common denominator" abstraction is possible between the two event streaming technologies.

The following opinions are taken:
 - Each producer produces 10 consecutively numbered messages to single topic. Depending on the partitioning on the topic, messages will be round-robined between the partitions.
 - The consumer group name needs to be provided
   - Kafka will automatically create topics and entries for Consumer Group offsets automatically, however Event Hubs will not - you will need to manually add the Topic and Consumer Groups.
   - Note that Kafka is case-sensitive. In Azure, almost all integration and storage names should be [lower cased](https://docs.microsoft.com/en-us/azure/architecture/best-practices/naming-conventions#storage)
 
 You'll also need to provide a storage account to Azure EH, as this is where the Consumer Group partition offsets are stored / remembered. Kafka stores the offsets on an internal Topic.
  
I've disabled the partition assignment event on the Kafka consumer, as there's a bug in the Confluent .net driver which then prohibits receiving of messages when the event is subscribed, but Kafka's partitioning 
assignments work very similarly to Azures - as a consumer on the same Consumer Group Connects or disconnects, the partitions on the topic are reallocated between the surviving consumers.
 