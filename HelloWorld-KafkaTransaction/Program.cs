using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace HelloWorld_KafkaTransaction
{
    class Program
    {
        public static ILogger SetupLog() {
            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder
                    .AddFilter("HelloWorld_KafkaTransaction", LogLevel.Trace)
                    .AddConsole();

            });
            ILogger logger = loggerFactory.CreateLogger<Program>();
            logger.LogInformation("############# Init ###############");
            return logger;
        }

        static async Task Main(string[] args)
        {
            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            var logger = SetupLog();

            //SampleProducer and Transaction Consumer, both uses msg with Key(int) and Value(string) structure. 
            //If you change key or value structure, please review SampleProducer and ReadProcessWriteTransaction classes. 
            var producer = new SampleProducer(GetProducerConfigs(), logger);
            await producer.ProduceSampleData("input-topic");

            var process = new ReadProcessWriteTransaction(GetProducerConfigs(), GetConsumerConfigs(), logger);
            process.SimpleReadWriteTransaction("input-topic","output-topic",cts.Token);

            logger.LogInformation("############# End ###############");
            Console.ReadLine();
        }


        //In a real case, don't do it that way - Be smart, use config files and vaults.
        public static ProducerConfig GetProducerConfigs()
        {
            var config = new ProducerConfig();
            config.BootstrapServers = "<server address here>:9092";
            config.ClientId = "producerInstance";
            config.SslEndpointIdentificationAlgorithm = SslEndpointIdentificationAlgorithm.Https;
            config.SaslUsername = "<confluent cloud API Key>";
            config.SaslPassword = "<confluent cloud API Secret>";
            config.SecurityProtocol = SecurityProtocol.SaslSsl;
            config.SaslMechanism = SaslMechanism.Plain;
            return config;
        }

        //In a real case, don't do it that way - Be smart, use config files and vaults.
        public static ConsumerConfig GetConsumerConfigs()
        {
            var config = new ConsumerConfig();
            config.GroupId = "myConsumerGroup";
            config.ClientId = "consumerInstance01";
            
            //Importat attribute for transactions
            config.IsolationLevel = IsolationLevel.ReadCommitted;
            /////
            config.EnableAutoCommit = false;
            config.AutoOffsetReset = AutoOffsetReset.Earliest;
            config.BootstrapServers = "<server address here>:9092";
            config.SslEndpointIdentificationAlgorithm = SslEndpointIdentificationAlgorithm.Https;
            config.SaslUsername = "<confluent cloud API Key>";
            config.SaslPassword = "<confluent cloud API Secret>";
            config.SecurityProtocol = SecurityProtocol.SaslSsl;
            config.SaslMechanism = SaslMechanism.Plain;
            return config;
        }
    }
}
