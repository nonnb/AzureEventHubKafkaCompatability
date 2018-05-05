using System;

namespace Common
{
    public interface IMessageConsumer
    {
        void Subscribe(Action<string> callback);
        void Stop();
        event EventHandler<string> OnPartitionsAssignedEvent;
        event EventHandler<string> OnError;
    }
}