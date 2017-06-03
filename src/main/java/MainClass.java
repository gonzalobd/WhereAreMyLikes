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

public class MainClass {

    public static String TOPIC = "like";
    private static final AtomicBoolean closed = new AtomicBoolean(false);
    public static ArrayList<Map<String, Map<String, Object>>> picsReceived = new ArrayList<Map<String, Map<String, Object>>>();
    public static ArrayList<Map<String,Object>> coordinatesSent = new ArrayList<Map<String,Object>>();
    public static String newline = System.getProperty("line.separator");
    public static String latitude;
    public static String longitude;


    public static void main(String[] args) {
        String KAFKA_HOST = args[0];
        String token=args[1];
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

        //Para guardar el estado que teniamos sobre las coordenadas que ya han sido enviados
        //utilizamos la serializacion de objetos java Kryo. Primero intentamos abrir
        //el fichero con las coordenadas enviadas (si existe, si no partimos de null)
        //Comento esta parte, ya que esto seria para producción y ahora estoy haciendo pruebas:
        /**
         try {
         input = new Input(new FileInputStream("hdfs://path-in-hdfs/coordinatesSent.bin"));
         likesSent = kryo.readObject(input, ArrayList.class);
         input.close();
         } catch (FileNotFoundException e) {
         System.out.println("File created");
         }
         */

        while (!closed.get()) {
            ConsumerRecords<String, Map<String, Object>> records = consumer.poll(100);
            for (ConsumerRecord<String, Map<String, Object>> record : records) {
                String id = record.value().get("id").toString();
                String stringUrl = "https://api.instagram.com/v1/users/" + id + "/media/recent/?access_token="+token;

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

                                        Map<String,Object> coordinateToSave =new HashMap<String,Object>();
                                        coordinateToSave.put("id",id);
                                        coordinateToSave.put("latitude",latitude);
                                        coordinateToSave.put("longitude",longitude);

                                        if (!coordinatesSent.contains(coordinateToSave)){

                                                try {
                                                    //Esto deberia ser un path de HDFS
                                                    Files.write(Paths.get("/home/hadoop/IdeaProjects/WhereAreMyLikes/src/main/java/out.csv"),
                                                            (latitude + "," + longitude + newline).
                                                                    toString().getBytes(), StandardOpenOption.APPEND);
                                                    System.out.println("Coordinate saved in csv file: "+latitude + "," + longitude);
                                                } catch (IOException e) {
                                                    //e.printStackTrace();
                                                }
                                                coordinatesSent.add(coordinateToSave);

                                            }

                            }
                        }


                        }

                    } catch (IOException e) {
                        //e.printStackTrace();
                    }

                try {
                    Thread.sleep(10000);//hay que dosificar las peticiones, instagram nos permite 5000/hora
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                    //Como lo de guardar el estado es para produccion, comento esto:

                /**
                 try {
                 output = new Output(new FileOutputStream("hdfs://path-in-hdfs/coordinatesSent.bin"));
                 } catch (FileNotFoundException e) {
                 e.printStackTrace();
                 }
                 kryo.writeObject(output, coordinatesSent);
                 output.close();
                 */

            }
        }
        consumer.close();
    }
}
