using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common;
using Kafka;

namespace DistLogDemo
{
    public enum ProduceOrConsume
    {
        Produce,
        Consume
    }

    class Program
    {
        static async Task Main(string[] args)
        {
            if (args.Length < 2 || !Enum.TryParse<ProduceOrConsume>(args[0], true, out var produceConsumeType))
            {
                Console.WriteLine("Usage: DistLogDemo {{produce|consume}} Topic(,s) [ConsumerGroupName]/[PartitionKey]");
                return;
            }

            var topics = args[1];
            var consumerGroupOrPartitionKey = args.Length >= 3
                ? args[2]
                : null;

            Console.WriteLine($"{produceConsumeType} to/from Kafka on Topic {topics}" + 
                              (produceConsumeType == ProduceOrConsume.Consume 
                                  ? $" for Group {consumerGroupOrPartitionKey}" 
                                  : $" with Partition Key {consumerGroupOrPartitionKey}"));

            if (produceConsumeType == ProduceOrConsume.Produce)
            {
                var producer = CreateProducer(topics);
                const int numMessages = 10;
                await Task.WhenAll(Enumerable.Range(0, numMessages)
                    .Select(i => producer.Produce($"Message #{i}", consumerGroupOrPartitionKey)));
                Console.WriteLine($"{numMessages} sent");
            }
            else
            {
                var consumer = CreateConsumer(topics.Split(","), consumerGroupOrPartitionKey ?? "$Default");
                consumer.Subscribe((msg, partId) => Console.WriteLine($"Message : {msg} received on Partition {partId}!"));
                consumer.OnPartitionsAssignedEvent += (sender, s) => Console.WriteLine(s);
                consumer.OnPartitionsRevokedEvent += (sender, s) => Console.WriteLine(s);
                consumer.OnError += (sender, err) => Console.WriteLine($"Error : {err}");
                Console.WriteLine("Consumer Listening - Enter to Quit");
                Console.ReadLine();
                consumer.Stop();
            }
        }

        private static IMessageConsumer CreateConsumer(IEnumerable<string> topicName, string consumerGroupName)
        {
            return new KafkaConsumer(topicName, consumerGroupName);
        }

        private static IMessageProducer CreateProducer(string topicName)
        {
            return new KafkaProducer(topicName);
        }
    }
}
