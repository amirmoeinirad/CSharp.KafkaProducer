using Confluent.Kafka;

namespace KafkaProducer
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("-----------------------------");
            Console.WriteLine("Apache Kafka Producer in .NET");
            Console.WriteLine("-----------------------------\n");


            // The 'ProducerConfig' class is part of the Confluent.Kafka library
            // and is used to configure the Kafka producer.
            // Here, we are setting the 'BootstrapServers' property to specify the Kafka broker address.
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

            // The 'ProducerBuilder' class is used to create a Kafka producer instance.
            // We specify the key and value types for the messages being produced.
            // In this case, we are using 'Null' for the key type and 'string' for the value type.
            using var producer = new ProducerBuilder<Null, string>(config).Build();

            try
            {
                // The 'ProduceAsync' method is used to send a message to the specified Kafka topic.
                // Input Parameters:
                // 1. Topic Name: "test-topic"
                // 2. Message: A new message with a null key and a string value "Hello Kafka from .NET!"
                var dr = await producer.ProduceAsync("test-topic", new Message<Null, string> { Value = "Hello Kafka from .NET!" });

                // If the message is successfully delivered, we print the delivery report details to the console.
                // The 'TopicPartitionOffset' property indicates where the message was stored in the Kafka topic.
                Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
            }
            catch (ProduceException<Null, string> e)
            {
                Console.WriteLine($"Delivery failed: {e.Error.Reason}");
            }


            Console.WriteLine("\nDone.");
        }
    }
}
