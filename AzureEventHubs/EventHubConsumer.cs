using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Common;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;

namespace AzureEventHubs
{
    public class EventHubConsumer : IMessageConsumer, IEventProcessor
    {
        private Action<string> _callback;
        private readonly EventProcessorHost _eventProcessorHost;
        public EventHubConsumer(string eventHubConnectionString, string topic,
            string storageConnectionString, string storageContainer)
        {
            _eventProcessorHost = new EventProcessorHost(
                topic,
                PartitionReceiver.DefaultConsumerGroupName,
                eventHubConnectionString,
                storageConnectionString,
                storageContainer);
        }

        public void Subscribe(Action<string> callback)
        {
            _callback = callback;
            _eventProcessorHost.RegisterEventProcessorAsync<EventHubConsumer>()
                .Wait();
        }

        public void Stop()
        {
            _eventProcessorHost.UnregisterEventProcessorAsync()
                .Wait();
        }

        public Task OpenAsync(PartitionContext context)
        {
            OnPartitionsAssignedEvent?.Invoke(this, $"{context.PartitionId}");
            return Task.CompletedTask;
        }

        public Task CloseAsync(PartitionContext context, CloseReason reason)
        {
            return Task.CompletedTask;
        }

        public Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            foreach (var eventData in messages)
            {
                _callback?.Invoke(Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count));
            }
            return Task.CompletedTask;
        }

        public Task ProcessErrorAsync(PartitionContext context, Exception error)
        {
            return Task.CompletedTask;
        }

        public event EventHandler<string> OnPartitionsAssignedEvent;
    }
}
