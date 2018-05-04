using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Common;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace Kafka
{
    public class KafkaConsumer : IMessageConsumer
    {
        private readonly Consumer<string, string> _consumer;
        private string _topic;
        private readonly IDeserializer<string> _keyDeserializer = new StringDeserializer(Encoding.UTF8);
        private readonly IDeserializer<string> _valueDeserializer = new StringDeserializer(Encoding.UTF8);
        private readonly CancellationTokenSource _cancellationTokenSource;

        public KafkaConsumer(string topic, string brokers, string consumerGroup = "DemoConsumerGroup")
        {
            _topic = topic;
            
            var flattenedConfig = new Dictionary<string, object>
            {
                {"bootstrap.servers", brokers},
                {"group.id", consumerGroup},
                {"enable.auto.commit", "false"},
                {"auto.offset.reset", "latest"}
            };

            _consumer = new Consumer<string, string>(
                flattenedConfig,
                _keyDeserializer,
                _valueDeserializer);

            _cancellationTokenSource = new CancellationTokenSource();
//            _consumer.OnPartitionsAssigned += (sender, list) =>
//            {
//                Task.Run(() => OnPartitionsAssignedEvent
//                    ?.Invoke(sender, string.Join(",", list.Select(p => p.Partition))));
//            };
            _consumer.OnError += (sender, error) => Console.WriteLine($"Error: {error.Reason}");
        }

        public void Subscribe(Action<string> callback)
        {
            _consumer.Subscribe(new[] { _topic });

            _consumer.OnMessage += (sender, message) =>
            {
                callback(message.Value);
            };

            Task.Run(() =>
            {
                var pollInterval = TimeSpan.FromSeconds(5);
                while (!_cancellationTokenSource.IsCancellationRequested)
                {
                    _consumer.Poll(pollInterval);
                }
            }, _cancellationTokenSource.Token);
        }

        public void Stop()
        {
            _cancellationTokenSource.Cancel();
        }

        public event EventHandler<string> OnPartitionsAssignedEvent;
    }
}
