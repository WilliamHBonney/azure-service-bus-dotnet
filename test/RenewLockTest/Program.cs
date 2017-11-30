using Microsoft.Extensions.Configuration;
using RenewLockTest.Common;
using RenewLockTest.Queue;
using RenewLockTest.Subscription;
using RenewLockTest.Topic;
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace RenewLockTest
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                while (true)
                {
                    Console.WriteLine("sending in 5 seconds");
                    for (var i = 0; i < 5; ++i)
                    {
                        Thread.Sleep(1000);
                        Console.Write(".");
                    }
                    Console.WriteLine();

                    ////Queue
                    //Queue_Receive();
                    //Queue_Send().Wait();

                    //Topic
                    Subscription_Receive();
                    Topic_Send().Wait();

                    Thread.Sleep(15 * 60 * 1000);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                Console.ReadLine();
            }
        }

        private static async Task Queue_Send()
        {
            var config = GetConfig();

            var settings = new AzureQueueSettings(
                connectionString: config["ServiceBus_ConnectionString"],
                queueName: config["ServiceBus_QueueName"]);

            var message = new TestMessage { Text = $"Hello Queue at {DateTime.Now.ToString()}" };

            var sender = new AzureQueueSender<TestMessage>(settings);
            await sender.SendAsync(message, null);

            Console.WriteLine($"Sent queue: {message.Text}");
        }

        private static void Queue_Receive()
        {
            var config = GetConfig();

            var settings = new AzureQueueSettings(
                connectionString: config["ServiceBus_ConnectionString"],
                queueName: config["ServiceBus_QueueName"]);

            var receiver = new AzureQueueReceiver<TestMessage>(settings);
            receiver.Receive(
                message =>
                {
                    Console.WriteLine($"START Received queue: {message.Text}");
                    Thread.Sleep(TimeSpan.FromMinutes(11));
                    Console.WriteLine($"FINISH Received queue: {message.Text}");

                    return MessageProcessResponse.Complete;
                },
                ex => { Console.WriteLine(ex.Message); },
                () => { Console.WriteLine("waiting for another message"); });
        }

        private static async Task Topic_Send()
        {
            var config = GetConfig();

            var settings = new AzureTopicSettings(
                connectionString: config["ServiceBus_ConnectionString"],
                topicName: config["ServiceBus_TopicName"]);

            var message = new TestMessage { Text = $"Hello Topic at {DateTime.Now.ToString()}" };

            var sender = new AzureTopicSender<TestMessage>(settings);
            await sender.SendAsync(message);

            Console.WriteLine("Sent");
        }

        private static void Subscription_Receive()
        {
            var config = GetConfig();

            var settings = new AzureSubscriptionSettings(
                connectionString: config["ServiceBus_ConnectionString"],
                topicName: config["ServiceBus_TopicName"],
                subscriptionName: config["ServiceBus_SubscriptionName"]);

            var receiver = new AzureSubscriptionReceiver<TestMessage>(settings);
            receiver.Receive(
                message =>
                {
                    Console.WriteLine($"START Received subscription: {message.Text}");
                    Thread.Sleep(TimeSpan.FromMinutes(11));
                    Console.WriteLine($"FINISH Received subscription: {message.Text}");

                    Console.WriteLine(message.Text);
                    return MessageProcessResponse.Complete;
                },
                ex => { Console.WriteLine(ex.Message); },
                () => { Console.WriteLine("Waiting..."); });
        }

        private static IConfigurationRoot GetConfig()
        {
            var builder = new ConfigurationBuilder()
                                .SetBasePath(Directory.GetCurrentDirectory())
                                .AddJsonFile("appsettings.json", optional: false);

            return builder.Build();
        }
    }
}
