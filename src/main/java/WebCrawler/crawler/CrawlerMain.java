package WebCrawler.crawler;

import WebCrawler.ad.Ad;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class CrawlerMain {
    private final static String IN_QUEUE_NAME = "distributed-crawler-queue-feed";
    private final static String OUT_QUEUE_NAME = "distributed-crawler-queue-product";
    private final static String ERR_QUEUE_NAME = "distributed-crawler-queue-error";

    private static AmazonCrawler crawler;
    private static ObjectMapper mapper;
    private static Channel outChannel;

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        if(args.length < 1)
        {
            System.out.println("Usage: Crawler <proxyFilePath>");
            System.exit(0);
        }
        mapper = new ObjectMapper();

        String proxyFilePath = args[0];

        String uri = System.getenv("CLOUDAMQP_URL");
        if (uri == null) uri = "amqp://jytyrkgt:9slXBZWUvB_pxzochwbOy7uSL1bKD9Ks@mosquito.rmq.cloudamqp.com/jytyrkgt";
        ConnectionFactory factory = new ConnectionFactory();
        try {
            factory.setUri(uri);
        } catch (URISyntaxException | NoSuchAlgorithmException | KeyManagementException e) {
            e.printStackTrace();
        }

        //Recommended settings
        factory.setRequestedHeartbeat(30);
        factory.setConnectionTimeout(30000);

        Connection connection1 = factory.newConnection();
        Channel inChannel = connection1.createChannel();
        // String queue = "distributed-crawler-queue-feed";
        inChannel.queueDeclare(IN_QUEUE_NAME, true, false, false, null);
        inChannel.basicQos(100); // Per consumer limit
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        Connection connection2 = factory.newConnection();
        outChannel = connection2.createChannel();
        // String queue = "distributed-crawler-queue-product";
        outChannel.queueDeclare(OUT_QUEUE_NAME, true, false, false, null);

        Connection connection3 = factory.newConnection();
        Channel errChannel = connection3.createChannel();
        // String queue = "distributed-crawler-queue-err";
        errChannel.queueDeclare(ERR_QUEUE_NAME, true, false, false, null);

        crawler = new AmazonCrawler(proxyFilePath, errChannel, ERR_QUEUE_NAME);


        Consumer consumer = new DefaultConsumer(inChannel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                    throws IOException {
                try {
                    String message = new String(body, "UTF-8");
                    System.out.println(" [x] Received '" + message + "'");
                    String[] fields = message.split(",");
                    String query = fields[0].trim();
                    double bidPrice = Double.parseDouble(fields[1].trim());
                    int campaignId = Integer.parseInt(fields[2].trim());
                    int queryGroupId = Integer.parseInt(fields[3].trim());

                    List<Ad> ads = crawler.GetAdBasicInfoByQuery(query, bidPrice, campaignId, queryGroupId);
                    for (Ad ad : ads) {
                        String jsonInString = mapper.writeValueAsString(ad);
                        System.out.println(jsonInString);
                        outChannel.basicPublish("", OUT_QUEUE_NAME, null, jsonInString.getBytes("UTF-8"));
                    }
                    Thread.sleep(2000);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        inChannel.basicConsume(IN_QUEUE_NAME, true, consumer);

    }
}
