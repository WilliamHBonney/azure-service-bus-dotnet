using Microsoft.Azure.ServiceBus;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace RenewLockTest.Queue
{
    //https://tahirnaushad.com/2017/08/20/azure-servicebus-in-net-core/
    public class AzureQueueSender<T> where T : class
    {
        public AzureQueueSender(AzureQueueSettings settings)
        {
            this.settings = settings;
            Init();
        }

        public async Task SendAsync(T item, Dictionary<string, object> properties)
        {
            var json = JsonConvert.SerializeObject(item);
            var message = new Message(Encoding.UTF8.GetBytes(json));

            if (properties != null)
            {
                foreach (var prop in properties)
                {
                    message.UserProperties.Add(prop.Key, prop.Value);
                }
            }

            await client.SendAsync(message);
        }

        private AzureQueueSettings settings;
        private QueueClient client;

        private void Init()
        {
            client = new QueueClient(this.settings.ConnectionString, this.settings.QueueName);
        }
    }
}
