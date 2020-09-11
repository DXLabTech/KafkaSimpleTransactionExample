using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace HelloWorld_KafkaTransaction
{
    public class ReadProcessWriteTransaction
    {
        private ProducerConfig _producerConfig;
        private ConsumerConfig _consumerConfig;
        private ILogger _logger;
        private List<TopicPartitionOffset> _offsetsToCommit;
        private TimeSpan _timeout => TimeSpan.FromSeconds(10);
        public ReadProcessWriteTransaction(ProducerConfig producerConfig, ConsumerConfig consumerConfig, ILogger logger)
        {
            _consumerConfig = consumerConfig;
            _producerConfig = producerConfig;
            //Transactional specific attributes
            _producerConfig.TransactionalId = "transactionalInstance01";
            _producerConfig.TransactionTimeoutMs = 12000;
            _producerConfig.EnableIdempotence = true;
            _logger = logger;
            _offsetsToCommit = new List<TopicPartitionOffset>();
        }

        //It is a dead simple example of Transacions on Apache Kafka. This is not a production ready code. 
        //To go further, you need to deal with rebalance scenarios and handle SetPartitionsRevokedHandler and SetPartitionsAssignedHandler
        //This is a 1:1 (1 read for 1 write) example. Read 1 message, process and commit the message
        public void SimpleReadWriteTransaction(string inputTopicName, string outputTopicName, CancellationToken ct)
        {
            using var consumer = new ConsumerBuilder<int, string>(_consumerConfig).Build();
            using var producer = new ProducerBuilder<int, string>(_producerConfig).Build();
            producer.InitTransactions(_timeout);
            consumer.Subscribe(inputTopicName);

            while (!ct.IsCancellationRequested)
            {
                
                producer.BeginTransaction();
                string currentProcessedMessage = "";
                //Read phase
                try
                {
                    var consumerResult = consumer.Consume(ct);
                    
                    //Process phase
                    currentProcessedMessage = ProcessMessage(consumerResult.Message);

                    //Write phase - Produce new message on output topic
                    producer.Produce(outputTopicName, new Message<int, string>() { Key = consumerResult.Message.Key, Value = currentProcessedMessage });


                    //Commit
                    producer.SendOffsetsToTransaction(new List<TopicPartitionOffset>() {
                                                        new TopicPartitionOffset(consumerResult.TopicPartition,consumerResult.Offset+1)
                                                        },
                                                            consumer.ConsumerGroupMetadata,
                                                            _timeout);
                    producer.CommitTransaction(_timeout);
                }
                catch (Exception e)
                {
                    _logger.LogError("Transaction Fault", e.Message, e.StackTrace);
                    producer.AbortTransaction(_timeout);
                }
                _logger.LogInformation($"Successful transaction: {currentProcessedMessage}");
                
            }



        }

        private string ProcessMessage(Message<int, string> kafkaMessage)
        {
            return kafkaMessage.Value + "-processed";
        }
    }
}
