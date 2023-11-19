import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.json.JSONObject.*;
import java.util.Arrays;

public class DataProducer {
    private Producer<String, String> producer;
    private String traceFileName;

    public DataProducer(Producer producer, String traceFileName) {
        this.producer = producer;
        this.traceFileName = traceFileName;
    }

    /**
      Task 1:
        In Task 1, you need to read the content in the tracefile we give to you, 
        create two streams, and feed the messages in the tracefile to different 
        streams based on the value of "type" field in the JSON string.

        Please note that you're working on an ec2 instance, but the streams should
        be sent to your samza cluster. Make sure you can consume the topics on the
        master node of your samza cluster before you make a submission.
    */
    public void sendData() {
        try (BufferedReader br = new BufferedReader(new FileReader(traceFileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                JSONObject json = new JSONObject(line);
                String type = json.getString("type");
                String topic = determineTopic(type);
                String key = String.valueOf(json.getInt("blockId")); // Partition based on blockId
                Integer my_partition = json.GetInteger("blockID") % 5;
                producer.send(new ProducerRecord<>(topic, my_partition, key, line));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private String determineTopic(String type) {
        if ("DRIVER_LOCATION".equals(type)) {
            return "driver-locations";
        } else if (Arrays.asList("LEAVING_BLOCK", "ENTERING_BLOCK", "RIDE_REQUEST", "RIDE_COMPLETE").contains(type)) {
            return "events";
        } else {
            throw new IllegalArgumentException("Unknown type: " + type);
        }
    }

   /* Determine field
   private String determineTopic(String type) {
        switch (type) {
            case "DRIVER_LOCATION":
                return "driver-locations";
            case "LEAVING_BLOCK":
            case "ENTERING_BLOCK":
            case "RIDE_REQUEST":
            case "RIDE_COMPLETE":
                return "events";
            default:
                throw new IllegalArgumentException("Unknown type: " + type);
        }
   }
   */
}
