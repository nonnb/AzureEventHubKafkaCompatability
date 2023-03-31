using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Common;
using Confluent.Kafka;

namespace Kafka
{
    public class KafkaConsumer : IMessageConsumer
    {
        private readonly IConsumer<string, string> _consumer;
        private HashSet<string> _topics;
        //private readonly IDeserializer<string> _keyDeserializer = new StringDeserializer(Encoding.UTF8);
        //private readonly IDeserializer<string> _valueDeserializer = new StringDeserializer(Encoding.UTF8);
        private readonly CancellationTokenSource _cancellationTokenSource;

        public KafkaConsumer(IEnumerable<string> topics, string consumerGroup)
        {
            _topics = new HashSet<string>(topics);

            var config = new ConsumerConfig
            {
                BootstrapServers = Config.KafkaBrokers,
                GroupId = consumerGroup,
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SessionTimeoutMs = 10000,
                // i.e. SAS not OAuth
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "$ConnectionString",
                SaslPassword = Config.ConnectionString,
                SslCaLocation = Config.Certificates,
                BrokerVersionFallback = "1.0.0"
            };

            _consumer = new ConsumerBuilder<string, string>(config)
                .SetPartitionsAssignedHandler((consumer, list) =>
                {
                    OnPartitionsAssignedEvent?.Invoke(consumer, $"Consumer {consumer.Name} has gained Partition(s) {string.Join(",", list.Select(p => p.Partition))}");
                })
                .SetPartitionsRevokedHandler((consumer, list) =>
                {
                    OnPartitionsRevokedEvent?.Invoke(consumer, $"Consumer {consumer.Name} has lost Partition(s) {string.Join(",", list.Select(p => p.Partition))}");
                })
                .SetErrorHandler((consumer, error) =>
                {
                    OnError?.Invoke(consumer, error.Reason);
                })
                .Build();

            _cancellationTokenSource = new CancellationTokenSource();
        }

        public void Subscribe(Action<string, int> callback)
        {
            _consumer.Subscribe(_topics);

            Task.Run(() =>
            {
                var pollInterval = TimeSpan.FromSeconds(5);
                while (!_cancellationTokenSource.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = _consumer.Consume(pollInterval);
                        if (consumeResult != null)
                        {
                            callback(consumeResult.Message.Value, consumeResult.Partition.Value);
                            _consumer.Commit(consumeResult);
                        }
                        else
                        {
                            Console.Write(".");
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }
                }
            }, _cancellationTokenSource.Token);
        }

        public void Stop()
        {
            _consumer.Close();
            _cancellationTokenSource.Cancel();
        }

        public event EventHandler<string> OnPartitionsAssignedEvent;
        public event EventHandler<string> OnPartitionsRevokedEvent;
        public event EventHandler<string> OnError;
    }
}
