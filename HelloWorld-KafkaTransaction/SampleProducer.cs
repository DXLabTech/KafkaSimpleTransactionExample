using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace HelloWorld_KafkaTransaction
{
    public class SampleProducer
    {
        private ProducerConfig _config;
        private ILogger _logger;
        public SampleProducer(ProducerConfig config, ILogger logger)
        {
            _config = config;
            _logger = logger;
        }

        public async Task ProduceSampleData(string topicName)
        {
            _logger.LogInformation("Writing sample data...");
            Stopwatch watch = new Stopwatch();
            watch.Start();
            using var p = new ProducerBuilder<int, string>(_config).Build();
            for (int i = 0; i < 100; i++)
            {
                try
                {

                    var delivery = await p.ProduceAsync(topicName, new Message<int, string> { Key = i, Value = $"message-{i}" });
                    _logger.LogInformation($"Message: {delivery.Value} on {delivery.TopicPartitionOffset}");
                }
                catch (Exception e)
                {
                    _logger.LogError($"Produce Sample Data Error - {e.Message}", e.Message, e.StackTrace);
                    break;
                }
            }
            watch.Stop();

            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                watch.Elapsed.Hours, watch.Elapsed.Minutes, watch.Elapsed.Seconds,
                watch.Elapsed.Milliseconds / 10);

            _logger.LogInformation($"Time elapsed for writing: {elapsedTime}");

        }
    }
}
