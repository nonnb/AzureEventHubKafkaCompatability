using System;
using System.Linq;
using System.Threading.Tasks;
using AzureEventHubs;
using Common;
using Kafka;

namespace DistLogDemo
{
    public enum DistLogType
    {
        Kafka,
        AzureEventHubs
    }

    public enum ProduceOrConsume
    {
        Produce,
        Consume
    }

    class Program
    {
        // Coming in C#7.1 static async Task Main(string[] args)
        static void Main(string[] args)
        {
            if (args.Length != 3
                || !Enum.TryParse<ProduceOrConsume>(args[0], true, out var produceConsumeType)
                || !Enum.TryParse<DistLogType>(args[1], true, out var distLogType))
            {
                Console.WriteLine($"Usage: dotnet DistLogDemo {{produce|consume}} {{kafka|azureeventhubs}} TopicName");
                return;
            }

            var topicName = args[2];
            Console.WriteLine($"{produceConsumeType} to/from {distLogType} on Topic {topicName}");

            if (produceConsumeType == ProduceOrConsume.Produce)
            {
                var producer = CreateProducer(distLogType, topicName);
                Task.WhenAll(Enumerable.Range(0, 10)
                    .Select(i => producer.Produce($"Message #{i}")))
                    .Wait();
            }
            else
            {
                var consumer = CreateConsumer(distLogType, topicName);
                consumer.Subscribe(msg => Console.WriteLine($"Message : {msg} received on Topic: {topicName}"));
                consumer.OnPartitionsAssignedEvent += (sender, s) => Console.WriteLine($"Partitions Assigned : {s}");
                Console.WriteLine("Consumer Listening - Enter to Quit");
                Console.ReadLine();
                consumer.Stop();
            }
        }

        private const string KafkaBrokers = "stubuntu.eastus.cloudapp.azure.com:9092";
        private const string AzureEventHubConnectionString = "Endpoint=sb://cteventhubs.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=UNSuxAxGH447VVU3rQ1DtSL8ToLjpQa1Y4wmORn12a0=";
        private const string OffsetStorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=eventhboffsets;AccountKey=nRFgG68bh1YkyzAxzxyYmxLhoEhtMZAJttOLqFjzJTmBsqQ3GoLLIA7TPQ8dT4BYfCrLqc0n5+rr9Ux3+Xuvzg==;EndpointSuffix=core.windows.net";
        private const string OffsetContainer = "huboffsets";


        private static IMessageConsumer CreateConsumer(DistLogType distLogType, string topicName)
        {
            return distLogType == DistLogType.Kafka
                ? (IMessageConsumer)new KafkaConsumer(topicName, KafkaBrokers, consumerGroup: $"DemoConsumer{Guid.NewGuid()}")
                : new EventHubConsumer(AzureEventHubConnectionString, topicName, OffsetStorageConnectionString,
                    OffsetContainer);
        }

        private static IMessageProducer CreateProducer(DistLogType distLogType, string topicName)
        {
            return distLogType == DistLogType.Kafka
                ? (IMessageProducer) new KafkaProducer(topicName, KafkaBrokers)
                : new EventHubProducer(AzureEventHubConnectionString, topicName);
        }
    }
}
