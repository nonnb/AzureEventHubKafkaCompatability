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
        // Coming in C#7.1 static async Task Main(string[] args) and then no .GetAwaiter().Wait() nonsense.
        static void Main(string[] args)
        {
            if (args.Length < 3
                || !Enum.TryParse<ProduceOrConsume>(args[0], true, out var produceConsumeType)
                || !Enum.TryParse<DistLogType>(args[1], true, out var distLogType))
            {
                Console.WriteLine($"Usage: dotnet DistLogDemo {{produce|consume}} {{kafka|azureeventhubs}} TopicName [ConsumerGroupName]");
                return;
            }

            var topicName = args[2];
            var consumerGroupName = args.Length >= 4
                ? args[3]
                : $"DemoConsumer{Guid.NewGuid()}";

            Console.WriteLine($"{produceConsumeType} to/from {distLogType} on Topic {topicName}" + 
                              (produceConsumeType == ProduceOrConsume.Consume 
                                  ? $" for Group {consumerGroupName}" 
                                  : ""));

            if (produceConsumeType == ProduceOrConsume.Produce)
            {
                var producer = CreateProducer(distLogType, topicName);
                Task.WhenAll(Enumerable.Range(0, 10)
                    .Select(i => producer.Produce($"Message #{i}")))
                    .Wait();
            }
            else
            {
                var consumer = CreateConsumer(distLogType, topicName, consumerGroupName);
                consumer.Subscribe(msg => Console.WriteLine($"Message : {msg} received on Topic: {topicName}"));
                consumer.OnPartitionsAssignedEvent += (sender, s) => Console.WriteLine($"Partitions Assigned : {s}");
                Console.WriteLine("Consumer Listening - Enter to Quit");
                Console.ReadLine();
                consumer.Stop();
            }
        }

        private const string KafkaBrokers = "YourKafkaHere:9092";
        // Easiest is to copy these from the appropriate resource on the Azure Portal
        private const string AzureEventHubConnectionString = "Endpoint=sb://xxx/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=xxx";
        private const string OffsetStorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=youreventhuboffsetsaccountname;AccountKey=xx;EndpointSuffix=core.windows.net";
        private const string OffsetContainer = "huboffsets";

        // Simple factory methods for either Kafka or AzEventHubs
        private static IMessageConsumer CreateConsumer(DistLogType distLogType, string topicName, string consumerGroupName)
        {
            return distLogType == DistLogType.Kafka
                ? (IMessageConsumer)new KafkaConsumer(topicName, KafkaBrokers, consumerGroupName)
                : new EventHubConsumer(AzureEventHubConnectionString, topicName, OffsetStorageConnectionString,
                    OffsetContainer, consumerGroupName);
        }

        private static IMessageProducer CreateProducer(DistLogType distLogType, string topicName)
        {
            return distLogType == DistLogType.Kafka
                ? (IMessageProducer) new KafkaProducer(topicName, KafkaBrokers)
                : new EventHubProducer(AzureEventHubConnectionString, topicName);
        }
    }
}
