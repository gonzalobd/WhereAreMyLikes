import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class JsonConsumer {
    public static String KAFKA_HOST = "localhost:9092";
    public static String TOPIC = "like";
    private static final AtomicBoolean closed = new AtomicBoolean(false);
    public static ArrayList<Map<String, Map<String, Object>>> picsReceived = new ArrayList<Map<String, Map<String, Object>>>();
    public static String newline = System.getProperty("line.separator");
    public static ArrayList<String> idsComputed = new ArrayList<>();
    public static String latitude;
    public static String longitude;

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Shutting down");
                closed.set(true);
            }
        });

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "serializers.JsonDeserializer");

        KafkaConsumer<String, Map<String, Object>> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        idsComputed.add(" ");
        while (!closed.get()) {
            ConsumerRecords<String, Map<String, Object>> records = consumer.poll(100);
            for (ConsumerRecord<String, Map<String, Object>> record : records) {
                String id = record.value().get("id").toString();

                if (!idsComputed.contains(id)) {

                    idsComputed.add(id);
                    String stringUrl = "https://api.instagram.com/v1/users/" + id + "/media/recent/?access_token=235583922.e029fea.8f0b40ca9ab9430d8544a5b67aa0bc2d";

                    try {

                        URL url = new URL(stringUrl);
                        URLConnection uc = url.openConnection();
                        BufferedReader br = new BufferedReader(new InputStreamReader((uc.getInputStream())));
                        ObjectMapper mapper = new ObjectMapper();
                        String json = br.readLine();
                        Map<String, Object> map = new HashMap<String, Object>();
                        // convert JSON string to Map
                        map = mapper.readValue(json, new TypeReference<Map<String, Object>>() {
                        });
                        picsReceived = (ArrayList<Map<String, Map<String, Object>>>) map.get("data");


                        for (Map<String, Map<String, Object>> pic : picsReceived) {

                            Map<String, Object> coordinates = new HashMap<String, Object>();
                            coordinates = pic.get("location");
                            try{
                                latitude=coordinates.get("latitude").toString();
                            } catch(Exception ex){
                                latitude="null";

                            }
                            try{
                                longitude=coordinates.get("longitude").toString();;
                            } catch(Exception ex){
                                longitude="null";
                            }


                            if (coordinates != null && !latitude.equals("null") && !longitude.equals("null")){


                                    if ((-90 < Double.parseDouble(String.valueOf(latitude))) &&
                                    (90 > Double.parseDouble(String.valueOf(latitude))) &&
                                    (-180 < Double.parseDouble(String.valueOf(longitude))) &&
                                    (180 > Double.parseDouble(String.valueOf(longitude)))) {

                                        System.out.println(latitude + "," + longitude);

                                        try {
                                            Files.write(Paths.get("/home/hadoop/IdeaProjects/WhereAreMyLikes/src/main/java/out.csv"),
                                                    (latitude + "," + longitude + newline).
                                                            toString().getBytes(), StandardOpenOption.APPEND);
                                        } catch (IOException e) {
                                            //exception handling left as an exercise for the reader
                                        }

                            }
                        }


                        }

                    } catch (IOException e) {
                        //e.printStackTrace();
                    }


                }
            }
        }
        consumer.close();
    }
}
