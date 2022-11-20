import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.fasterxml.jackson.databind.ObjectMapper;

@Path("/consumer")
public class Consumer {

private KafkaConsumer<String, String> kafkaConsumer;

public Consumer() {
kafkaConsumer = new KafkaConsumer<String, String>(Config.getConsumerConfig());
}

@GET
@Path("/{user}/messages")
@Produces(MediaType.APPLICATION_JSON)
public Response getMessages(@PathParam("user") String user) {
List<ConsumerRecord<String, String>> messages = Database.getMessages(user);
GenericEntity<List<ConsumerRecord<String, String>>> entity = new GenericEntity<List<ConsumerRecord<String, String>>>(messages) {};
return Response.ok(entity).build();
}

@GET
@Path("/{user}/subscribe/{topic}")
public Response subscribe(@PathParam("user") String user, @PathParam("topic") String topic) {
kafkaConsumer.subscribe(Config.getTopics(topic));
return Response.ok().build();
}

public void run() {
while (true) {
  ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
  for (ConsumerRecord<String, String> record : records) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      Status status = mapper.readValue(record.value(), Status.class);
      Database.saveMessage(user, status);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
}
}