﻿using Microsoft.Azure.ServiceBus;
using Newtonsoft.Json;
using RenewLockTest.Common;
using System;
using System.Text;
using System.Threading.Tasks;

namespace RenewLockTest.Queue
{
    public class AzureQueueReceiver<T> where T : class
    {
        public AzureQueueReceiver(AzureQueueSettings settings)
        {
            this.settings = settings;
            Init();
        }

        public void Receive(
            Func<T, MessageProcessResponse> onProcess,
            Action<Exception> onError,
            Action onWait)
        {
            var options = new MessageHandlerOptions(e =>
            {
                onError(e.Exception);
                return Task.CompletedTask;
            })
            {
                AutoComplete = false,
                MaxAutoRenewDuration = TimeSpan.FromMinutes(30),
                MaxConcurrentCalls = 1
            };

            client.RegisterMessageHandler(
                async (message, token) =>
                {
                    try
                    {
                        // Get message
                        var data = Encoding.UTF8.GetString(message.Body);
                        T item = JsonConvert.DeserializeObject<T>(data);

                        // Process message
                        var result = onProcess(item);

                        if (result == MessageProcessResponse.Complete)
                            await client.CompleteAsync(message.SystemProperties.LockToken);
                        else if (result == MessageProcessResponse.Abandon)
                            await client.AbandonAsync(message.SystemProperties.LockToken);
                        else if (result == MessageProcessResponse.Dead)
                            await client.DeadLetterAsync(message.SystemProperties.LockToken);

                        // Wait for next message
                        onWait();
                    }
                    catch (Exception ex)
                    {
                        await client.DeadLetterAsync(message.SystemProperties.LockToken);
                        onError(ex);
                    }
                }, options);
        }

        private AzureQueueSettings settings;
        private QueueClient client;

        private void Init()
        {
            client = new QueueClient(this.settings.ConnectionString, this.settings.QueueName);
        }
    }
}
