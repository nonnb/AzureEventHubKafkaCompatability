using System.Collections.Generic;
using System.Threading.Tasks;

namespace Common
{
    public interface IMessageProducer
    {
        Task<bool> Produce(string message, string publishKey = null);
    }
}
