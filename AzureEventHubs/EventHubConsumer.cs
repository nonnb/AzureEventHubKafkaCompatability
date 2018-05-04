using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Common;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;

namespace AzureEventHubs
{
    public class EventProcessor : IEventProcessor
    {
        private readonly EventHubConsumer _consumer;

        public EventProcessor(EventHubConsumer consumer)
        {
            _consumer = consumer;
        }

        public Task OpenAsync(PartitionContext context)
        {
            _consumer.FireOnPartitionsAssignedEvent($"{context.PartitionId}");
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
                _consumer.OnMessageReceived(Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count));
            }
            return Task.CompletedTask;
        }

        public Task ProcessErrorAsync(PartitionContext context, Exception error)
        {
            return Task.CompletedTask;
        }

    }

    public class EventHubConsumer : IMessageConsumer, IEventProcessorFactory
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
            _eventProcessorHost.RegisterEventProcessorFactoryAsync(this)
                .Wait();
        }

        public void Stop()
        {
            _eventProcessorHost.UnregisterEventProcessorAsync()
                .Wait();
        }

        internal void FireOnPartitionsAssignedEvent(string partitionsAssigned)
        {
            OnPartitionsAssignedEvent?.Invoke(this, partitionsAssigned);
        }

        internal void OnMessageReceived(string message)
        {
            _callback?.Invoke(message);
        }

        public event EventHandler<string> OnPartitionsAssignedEvent;

        public IEventProcessor CreateEventProcessor(PartitionContext context)
        {
            return new EventProcessor(this);
        }
    }
}
