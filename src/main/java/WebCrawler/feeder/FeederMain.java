package WebCrawler.feeder;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class FeederMain {
    private final static String QUEUE_NAME = "distributed-crawler-queue-feed";

    public static void main(String[] args) throws Exception {

        String rawQueryDataFilePath = args[0];

        try (BufferedReader br = new BufferedReader(new FileReader(rawQueryDataFilePath))) {

            String line;

            String uri = System.getenv("CLOUDAMQP_URL");
            if (uri == null) uri = "amqp://jytyrkgt:9slXBZWUvB_pxzochwbOy7uSL1bKD9Ks@mosquito.rmq.cloudamqp.com/jytyrkgt";

            ConnectionFactory factory = new ConnectionFactory();
            factory.setUri(uri);

            //Recommended settings
            factory.setRequestedHeartbeat(30);
            factory.setConnectionTimeout(30000);

            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            // String queue = "distributed-crawler-queue";
            boolean durable = true;    //durable - RabbitMQ will never lose the queue if a crash occurs
            boolean exclusive = false;  //exclusive - if queue only will be used by one connection
            boolean autoDelete = false; //autodelete - queue is deleted when last consumer unsubscribes
            channel.queueDeclare(QUEUE_NAME, durable, exclusive, autoDelete, null);

            while ((line = br.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                String exchangeName = "";
                String routingKey = QUEUE_NAME;
                channel.basicPublish(exchangeName, routingKey, null, line.getBytes("UTF-8"));
                System.out.println(" [x] Sent '" + line + "'");
            }
            channel.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
