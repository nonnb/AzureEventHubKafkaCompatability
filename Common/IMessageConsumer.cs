using System;

namespace Common
{
    public interface IMessageConsumer
    {
        void Subscribe(Action<string, int> callback);
        void Stop();
        event EventHandler<string> OnPartitionsAssignedEvent;
        event EventHandler<string> OnPartitionsRevokedEvent;
        event EventHandler<string> OnError;
    }
}