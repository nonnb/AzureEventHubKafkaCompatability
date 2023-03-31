using System;
using System.Threading;
using System.Threading.Tasks;
using Common;
using Confluent.Kafka;

namespace Kafka
{
    public class KafkaProducer : IMessageProducer
    {
        private readonly IProducer<string, string> _kafkaProducer;
        private readonly string _topicName;
        //private readonly ISerializer<string> _keySerializer = new StringSerializer(Encoding.UTF8);
        //private readonly ISerializer<string> _valueSerializer = new StringSerializer(Encoding.UTF8);

        public KafkaProducer(string topicName)
        {
            _topicName = topicName;
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = Config.KafkaBrokers,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "$ConnectionString",
                SaslPassword = Config.ConnectionString,
                SslCaLocation = Config.Certificates,
            };
            _kafkaProducer = new ProducerBuilder<string, string>(producerConfig)
                .Build();
        }

        public async Task<bool> Produce(string message, string partitionKey = null)
        {
            try
            {
                var result = await _kafkaProducer.ProduceAsync(_topicName, new Message<string, string>
                {
                    Key = partitionKey,
                    Value = message,
                    Headers = new Headers()
                }, CancellationToken.None);
                _kafkaProducer.Flush(TimeSpan.FromSeconds(1));
                return result.Status == PersistenceStatus.Persisted;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }
    }
}
