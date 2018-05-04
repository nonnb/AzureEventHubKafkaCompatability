using System.Text;
using System.Threading.Tasks;
using Common;
using Microsoft.Azure.EventHubs;

namespace AzureEventHubs
{
    public class EventHubProducer : IMessageProducer
    {
        private readonly EventHubClient _eventHubClient;

        public EventHubProducer(string connectionString, string topicName)
        {
            var connBulder = new EventHubsConnectionStringBuilder(connectionString)
            {
                EntityPath = topicName
            };
            _eventHubClient = EventHubClient.CreateFromConnectionString(connBulder.ToString());
        }

        public async Task<bool> Produce(string message, string publishKey = null)
        {
            if (publishKey != null)
            {
                await _eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(message)), publishKey);
            }
            else
            {
                await _eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(message)));
            }
            return true;
        }
    }
}