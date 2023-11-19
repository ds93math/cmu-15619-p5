import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

public class DataProducerRunner {

    public static void main(String[] args) throws Exception {
        /*
            Tasks to complete:
            - Write enough tests in the DataProducerTest.java file
            - Instantiate the Kafka Producer by following the API documentation
            - Instantiate the DataProducer using the appropriate trace file and the producer
            - Implement the sendData method as required in DataProducer
            - Call the sendData method to start sending data
        */
        String traceFileName = System.getenv("TRACE_FILENAME");
        String traceFileName2 = System.getenv("TRACE2_FILENAME");

        if (traceFileName2 == null || traceFileName2.isEmpty()) {
            System.err.println("TRACE2_FILENAME environment variable is not set.");
            System.exit(1);
        }

        // Set up the properties for the Kafka producer
        Properties props = new Properties();
        props.put("bootstrap.servers", "ec2-44-211-52-191.compute-1.amazonaws.com:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
               
        // Create the Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // Instantiate the DataProducer with the Kafka producer and trace file name
        DataProducer dataProducer = new DataProducer(producer, traceFileName2);

        // Call sendData to start sending data
        dataProducer.sendData();
    }
}
