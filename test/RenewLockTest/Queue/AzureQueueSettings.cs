using System;

namespace RenewLockTest.Queue
{
    //https://tahirnaushad.com/2017/08/20/azure-servicebus-in-net-core/
    public class AzureQueueSettings
    {
        public AzureQueueSettings(string connectionString, string queueName)
        {
            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException("connectionString");

            if (string.IsNullOrEmpty(queueName))
                throw new ArgumentNullException("queueName");

            this.ConnectionString = connectionString;
            this.QueueName = queueName;
        }

        public string ConnectionString { get; }
        public string QueueName { get; }
    }
}
