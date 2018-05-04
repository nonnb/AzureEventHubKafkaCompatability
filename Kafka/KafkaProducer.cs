using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Common;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace Kafka
{
    public class KafkaProducer : IMessageProducer
    {
        private readonly Producer<string, string> _kafkaProducer;
        private readonly string _topicName;
        private readonly ISerializer<string> _keySerializer = new StringSerializer(Encoding.UTF8);
        private readonly ISerializer<string> _valueSerializer = new StringSerializer(Encoding.UTF8);

        public KafkaProducer(string topicName, string brokers)
        {
            _topicName = topicName;
            var config = new[]
            {
                new KeyValuePair<string, object>("bootstrap.servers", brokers)
            };
            _kafkaProducer = new Producer<string, string>(
                config,
                _keySerializer,
                _valueSerializer);
            _kafkaProducer.OnError += (sender, error) => Console.WriteLine(error.Reason);
        }

        public async Task<bool> Produce(string message, string publishKey = null)
        {
            try
            {
                var result = await _kafkaProducer.ProduceAsync(_topicName, publishKey, message);
                _kafkaProducer.Flush(1000);
                return !(result.Error.HasError);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }
    }
}
