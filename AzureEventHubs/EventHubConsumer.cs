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
            _consumer.FireOnPartitionsAssignedEvent($"Closed: {context.PartitionId}");
            return Task.CompletedTask;
        }

        public Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            foreach (var eventData in messages)
            {
                _consumer.OnMessageReceived(Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count));
            }
            return context.CheckpointAsync();
        }

        public Task ProcessErrorAsync(PartitionContext context, Exception error)
        {
            _consumer.FireOnError($"Error - Partition: {context.PartitionId} - {error.Message}");
            return Task.CompletedTask;
        }
    }

    public class EventHubConsumer : IMessageConsumer, IEventProcessorFactory
    {
        private Action<string> _callback;
        private readonly EventProcessorHost _eventProcessorHost;
        public EventHubConsumer(string eventHubConnectionString, string topic,
            string storageConnectionString, string storageContainer, string consumerGroupName)
        {
            _eventProcessorHost = new EventProcessorHost(
                topic,
                consumerGroupName,
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

        internal void FireOnError(string error)
        {
            OnError?.Invoke(this, error);
        }

        internal void OnMessageReceived(string message)
        {
            _callback?.Invoke(message);
        }

        public event EventHandler<string> OnPartitionsAssignedEvent;
        public event EventHandler<string> OnError;

        public IEventProcessor CreateEventProcessor(PartitionContext context)
        {
            return new EventProcessor(this);
        }
    }
}
