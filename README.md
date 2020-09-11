# KafkaSimpleTransactionExample
Repository with example of transaction processing using read-process-write pattern with apache kafka.

This is the simplest example of using transactions in Apache Kafka that I could think of.

It is about consumption and production 1:1 between different topics.

input-topic -> process -> output-topic

So, the read message on input-topic is commited on the transaction together 

The message that was read in the input topic is only confirmed together with the message produced in the output topic.
These two commits are part of the same transactional context.
