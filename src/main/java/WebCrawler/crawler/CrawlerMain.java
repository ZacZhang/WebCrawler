package WebCrawler.crawler;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;

import java.io.IOException;
import java.nio.channels.Channel;
import java.util.List;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import WebCrawler.ad.Ad;

public class CrawlerMain {
    private final static String IN_QUEUE_NAME = "distributed-crawler-queue-feed";
    private final static String OUT_QUEUE_NAME = "distributed-crawler-queue-product";
    private final static String ERR_QUEUE_NAME = "distributed-crawler-queue-error";

    private static AmazonCrawler crawler;
    private static ObjectMapper mapper;
    //private static BufferedWriter bw;
    private static Channel outChannel;
    private static Channel errChannel;

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        if(args.length < 1)
        {
            System.out.println("Usage: Crawler <proxyFilePath>");
            System.exit(0);
        }
        mapper = new ObjectMapper();
        // String rawQueryDataFilePath = args[0];
        // String adsDataFilePath = args[1];
        String proxyFilePath = args[0];
        // String logFilePath = args[3];



        AmazonCrawler crawler = new AmazonCrawler(proxyFilePath, logFilePath);
        File file = new File(adsDataFilePath);
        // if file doesnt exists, then create it
        if (!file.exists()) {
            file.createNewFile();
        }

        FileWriter fw = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);
        try (BufferedReader br = new BufferedReader(new FileReader(rawQueryDataFilePath))) {

            String line;
            while ((line = br.readLine()) != null) {
                if(line.isEmpty())
                    continue;
                System.out.println(line);
                String[] fields = line.split(",");
                String query = fields[0].trim();
                double bidPrice = Double.parseDouble(fields[1].trim());
                int campaignId = Integer.parseInt(fields[2].trim());
                int queryGroupId = Integer.parseInt(fields[3].trim());
                List<Ad> ads =  crawler.GetAdBasicInfoByQuery(query, bidPrice, campaignId, queryGroupId);
                for(Ad ad : ads) {
                    String jsonInString = mapper.writeValueAsString(ad);
                    //System.out.println(jsonInString);
                    bw.write(jsonInString);
                    bw.newLine();
                }
                Thread.sleep(5000);
            }
            bw.close();
        }catch(InterruptedException ex) {
            Thread.currentThread().interrupt();
        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        crawler.cleanup();
    }
}
