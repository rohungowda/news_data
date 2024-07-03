package org.example;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.sql.*;
import java.sql.Date;
import java.util.*;

public class producer extends Thread {

    private boolean paused = false;

    private final KafkaProducer<String, String> kafka_producer;
    private final String topic_name;

    private final String query = "WITH batch_rows AS (" +
                                        " DELETE FROM log_news_table" +
                                        " WHERE topic = ?" +
                                        " RETURNING log_id, id" +
                                        ")" +
                                        " SELECT mnt.* FROM main_news_table mnt JOIN" +
                                        " batch_rows br ON mnt.id = br.id";

    private final Connection conn;

    private final int CHUNK_SIZE = 200;


    public producer(String topic_name, data_connector instance) throws  SQLException{
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("transactional.id", UUID.randomUUID().toString());
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.kafka_producer = new KafkaProducer<>(props);
        this.topic_name = topic_name;
        this.conn = instance.create_connection();

        Statement statement = this.conn.createStatement();
        statement.execute("LISTEN batch_added");
        statement.close();
    }

    public Connection getConn() {
        return conn;
    }

    public void send_batch(ResultSet results){
        kafka_producer.initTransactions();
        try {
            kafka_producer.beginTransaction();
            //
            // have to batch data from result set, if no data skip over
            // have to send data through jsonobjects so have to change value.seralizer
            // Arvo Serializer as well
            //
            String topic_data = "placeholder";
            ProducerRecord<String,String> record = new ProducerRecord<>(topic_name, null,topic_data);
            kafka_producer.send(record);
            kafka_producer.commitTransaction();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // ProducerFencedException: Another producer with the same 'transactional.id' is active.
            // OutOfOrderSequenceException: Attempted to send messages out of order within a transaction.
            // AuthorizationException: Client is not authorized to perform the requested operation.
            kafka_producer.close();
        } catch (KafkaException e) {
            kafka_producer.abortTransaction();
        }
    }

    public void collect_data() throws SQLException {
        PreparedStatement ps = this.conn.prepareStatement(query);

        ps.setString(1,topic_name);
        ResultSet resultSet =  ps.executeQuery();
        while(resultSet.next()){
            String id = resultSet.getString("id");
            String topicName = resultSet.getString("topic");
            Date timestamp = resultSet.getDate("timestamp");
            String article = resultSet.getString("article");

            //Print the results
            System.out.println("ID: " + id);
            System.out.println("Topic Name: " + topicName);
            System.out.println("Timestamp: " + timestamp);
            System.out.println("Article: " + article);
            System.out.println("--------------------------");

        }

    }


    public  List<ObjectNode> test_threads(){
        List<ObjectNode> list = new ArrayList<>();

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode jsonObject = mapper.createObjectNode();

        String Article = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed sit amet accumsan justo. Vestibulum sodales, nisi eget condimentum sollicitudin, risus odio mollis nisl, quis tincidunt nulla nisi eget tortor. Proin ut mi augue. Integer dignissim, mi eget tempus facilisis, nisi ipsum sagittis elit, at finibus turpis eros at purus. In et eleifend nisi, vel condimentum libero. Curabitur sit amet nisi sapien. Aenean convallis vestibulum justo, vitae fermentum sem sollicitudin a. Nam fermentum sem id elit malesuada, vitae fermentum mi vestibulum. Nulla vel molestie magna. Duis ultricies purus in turpis interdum, et euismod nisi sagittis.\n" +
                "\n" +
                "Nam nec justo vitae urna sodales vestibulum. Sed laoreet ligula id ligula fermentum tincidunt. Ut congue felis libero, ac venenatis leo interdum eget. Vestibulum egestas quam nec tellus gravida posuere. Sed sit amet ultricies turpis. Quisque sagittis semper turpis, eu egestas arcu tincidunt et. Sed pretium tellus nulla, ut lacinia elit posuere id. Nullam a turpis a leo pharetra fermentum. Sed non ipsum ultrices, suscipit odio in, scelerisque velit. Ut auctor orci id lectus tincidunt suscipit.\n" +
                "\n" +
                "Vivamus rutrum varius orci, vitae hendrerit lorem malesuada eget. Curabitur vestibulum ante in purus consectetur, eu lacinia mi eleifend. Nulla sed leo nisl. Integer ut ante quis lorem tincidunt tristique. Nam non fringilla velit. Duis sollicitudin elementum risus vel fermentum. Nullam nec laoreet nisl. Suspendisse potenti. In hac habitasse platea dictumst. Vestibulum ac mauris in tortor tristique posuere. Vivamus tincidunt quam vel neque malesuada, et gravida nulla ullamcorper.\n" +
                "\n" +
                "Phasellus gravida efficitur erat nec ultricies. Sed convallis sit amet purus et mollis. Proin malesuada velit id magna tristique, sed blandit nulla hendrerit. Nulla facilisi. Mauris eget elit id ipsum aliquam euismod in sed elit. Integer maximus erat risus, sed congue ex tincidunt sed. Phasellus at justo vel nulla rhoncus accumsan. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Phasellus non velit nisi. Vivamus convallis lectus quis ex condimentum scelerisque. Sed et magna sapien. Suspendisse potenti. Proin condimentum tellus eget aliquam blandit.\n" +
                "\n" +
                "Donec volutpat nisi vel lorem blandit vestibulum. Fusce pretium purus ac massa viverra, sit amet efficitur est feugiat. Nulla facilisi. Nullam vel sollicitudin nunc, ac rhoncus odio. Proin sed fringilla turpis. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Nulla lobortis ipsum ligula, ac lobortis libero molestie at. Cras lacinia ante sit amet lorem ultricies, vel interdum dui ultricies. Nam ullamcorper enim vitae ultricies interdum.\n" +
                "\n" +
                "Pellentesque ultrices odio vel ligula bibendum lobortis. In at placerat mi. Donec dapibus arcu at scelerisque commodo. Aliquam vitae pulvinar lorem. Mauris fermentum est ut risus commodo lobortis. Donec malesuada imperdiet nisl, in malesuada urna mattis sit amet. Nullam nec lectus venenatis, posuere felis at, volutpat mauris. Nulla facilisi. Mauris pretium, sem ut vehicula faucibus, velit nisl scelerisque eros, at venenatis mi metus a ante. Nullam lacinia tellus in sapien congue, id malesuada mi fermentum. Integer lobortis nibh ut convallis vehicula. Nulla viverra justo nisi, non consequat eros consectetur sed. In auctor, nisi eget venenatis posuere, nisl libero maximus lectus, sit amet luctus odio sem nec libero.\n" +
                "\n" +
                "Maecenas consequat tortor in nisl viverra, sit amet lacinia enim tincidunt. Suspendisse dictum urna quis diam viverra, nec venenatis magna scelerisque. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Duis et malesuada nunc, ut condimentum ipsum. Mauris condimentum eros sit amet felis feugiat, vel lacinia dolor placerat. Curabitur sit amet est a dui varius facilisis. Aenean vel magna enim. Morbi vehicula enim eu dapibus dictum. Vestibulum consectetur consequat lectus, sed laoreet ligula mollis eu. Ut aliquam, velit eu tincidunt mollis, elit metus congue orci, in tincidunt nisi tellus id dui. Donec ultricies ante et augue ullamcorper, eu fermentum felis mattis.\n" +
                "\n" +
                "Vivamus feugiat tellus ut bibendum pharetra. Proin ut nulla euismod, pharetra leo eu, vestibulum metus. Quisque eu elit sed arcu auctor fermentum. Aliquam erat volutpat. Suspendisse potenti. Sed pretium vestibulum sapien, a varius lorem varius vitae. In hac habitasse platea dictumst. Donec non leo non justo vestibulum iaculis. Sed nec feugiat nisi. Nulla facilisi. Duis congue ex ligula, vitae scelerisque dui tincidunt a. Curabitur a lacinia urna. Fusce ac libero at nisi laoreet venenatis id eu erat. Sed viverra, orci vel volutpat scelerisque, mi ligula tristique ligula, at iaculis augue elit a elit. Nam id sodales ligula. Ut at aliquam dui.\n" +
                "\n" +
                "Integer posuere leo eget tortor scelerisque laoreet. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Nam fermentum quam nec erat vehicula, ac ultricies nunc tristique. Proin lacinia elit sed eros volutpat, vel suscipit nisi lacinia. Donec sit amet ipsum ut dui placerat tristique. Pellentesque ac fermentum dui. Nunc tristique massa at efficitur euismod. In hac habitasse platea dictumst. Aliquam pulvinar blandit urna vel auctor. Aenean vestibulum ex a neque venenatis, et mattis orci posuere. Fusce efficitur sapien non tortor faucibus, id consequat sem luctus. Nullam sodales tellus nec tellus ultricies interdum.\n" +
                "\n" +
                "Duis id nunc sem. Proin dapibus mauris at lacus commodo, nec pharetra lectus ultrices. Quisque vehicula purus sit amet est elementum, id lobortis enim condimentum. Fusce vel nisi et odio lacinia lacinia. Donec blandit consequat nisl, a pharetra erat suscipit ac. Mauris euismod hendrerit nisi non dapibus. Integer posuere ex nec libero fermentum, sed eleifend purus tristique. Nulla id odio vestibulum, ultrices risus vel, luctus ligula. Sed eget odio sapien. Nam vitae eros nec nunc lacinia vulputate. Suspendisse scelerisque velit id ex ultricies feugiat. Donec aliquet placerat justo, et hendrerit orci lobortis a. Donec nec nunc eget eros vestibulum accumsan.\n" +
                "\n" +
                "Phasellus tincidunt arcu quis venenatis faucibus. Duis elementum nibh in orci sodales auctor. Ut id odio orci. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Nullam ac ex vel nulla varius consequat. Nulla quis arcu nec purus egestas venenatis. Nulla vel suscipit mi. Ut ultricies libero ut dui lobortis, id gravida ante interdum. Suspendisse a ipsum vitae ipsum convallis convallis. Curabitur feugiat volutpat tortor, sed egestas dui vestibulum a. Sed nec libero sed mauris feugiat volutpat. Suspendisse fermentum lectus a est tempus luctus. Nam dignissim aliquet eros, non congue nulla ultricies non.\n" +
                "\n" +
                "Pellentesque pulvinar orci vel nunc dapibus, in vestibulum tellus mattis. Mauris accumsan vel ex in congue. Morbi varius mi at lectus congue, a molestie ipsum dignissim. Donec sit amet erat eget elit malesuada congue. Integer placerat nibh in lorem faucibus, quis tincidunt nulla placerat. Proin dignissim metus sit amet risus faucibus, auctor tempus enim placerat. Mauris vitae justo non est sodales laoreet. Suspendisse congue fringilla urna, non laoreet erat congue non. Ut a felis feugiat, convallis lectus vel, condimentum lorem.\n" +
                "\n" +
                "Sed convallis mollis augue vel ultricies. Duis tincidunt neque quis felis vehicula, at bibendum ligula cursus. Morbi pulvinar, libero eget sollicitudin vestibulum, nisi risus suscipit purus, ut condimentum odio mi at leo. Donec aliquet diam et arcu suscipit, id interdum risus condimentum. Nullam euismod lectus at orci venenatis, et aliquet libero ultrices. Donec malesuada, mi at fringilla lacinia, lacus neque viverra ante, et auctor dolor arcu vel dui. Duis faucibus odio vel ligula posuere, eget fermentum mi hendrerit.\n" +
                "\n" +
                "Cras vitae quam non nisi eleifend suscipit. Nulla facilisi. Proin aliquam sem vel ex iaculis rhoncus. Quisque faucibus pharetra leo non dapibus. Duis a risus dapibus, suscipit tellus in, tristique est. Vestibulum ac leo id purus tempor ultricies. Aliquam et magna aliquet, convallis odio et, dignissim lacus. Integer quis sodales nulla. Pellentesque ac lorem eu ex bibendum finibus in quis elit. Mauris tincidunt, justo id consequat vehicula, justo elit sagittis eros, ut tincidunt libero mauris sed ligula. Nulla facilisi. Phasellus in libero ligula. Curabitur gravida sit amet magna nec lobortis. Suspendisse potenti.\n" +
                "\n" +
                "Praesent non turpis ut elit tincidunt vehicula. Nam venenatis purus nec sollicitudin commodo. Nullam aliquet diam eget metus rhoncus, non rhoncus dolor tempor. Morbi non magna sed orci suscipit convallis at sit amet sapien. Suspendisse potenti. Proin vel molestie risus. Nulla facilisi. Etiam aliquet ex vel lorem aliquet, at laoreet felis mollis. Sed vitae pharetra nisi, non aliquet dui. Aliquam erat volutpat. Fusce ut leo velit. Curabitur nec venenatis velit, nec ullamcorper libero. Fusce sed sapien id ipsum rutrum accumsan a vel dui. Sed et interdum est. Proin sagittis metus ac orci feugiat, auctor eleifend justo bibendum. Vivamus convallis accumsan ligula.\n" +
                "\n" +
                "In vulputate eros velit, in sodales arcu interdum in. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Nunc non dapibus sapien. Nulla facilisi. Ut elementum tristique arcu, a tincidunt eros dapibus et. Sed ac suscipit nulla. Ut quis hendrerit mauris. Nullam eleifend nisi in sem tincidunt, id scelerisque risus aliquet. Mauris placerat ligula at est scelerisque, sed tincidunt orci tempor. Maecenas eget condimentum magna, sit amet congue est. Vestibulum vehicula augue non velit placerat, id molestie felis dapibus. Curabitur eu sapien nec enim dignissim rutrum. Ut tempus, mi et ultricies vehicula, libero odio lacinia lacus, ac fermentum libero metus non ante. Fusce consectetur faucibus ligula, nec tincidunt libero mollis nec. Morbi vitae mauris sem.\n" +
                "\n" +
                "Proin id ipsum nisl. Suspendisse potenti. Vestibulum vestibulum, ligula ut vehicula consequat, odio felis fermentum risus, eu placerat purus lectus eu metus. Ut rutrum ligula vel nunc congue aliquet. In vel ex eu ex viverra lacinia. Pellentesque dapibus lorem id purus eleifend, sit amet vestibulum mi efficitur. Ut condimentum, eros quis venenatis congue, ipsum quam auctor risus, nec laoreet nisi libero id lorem. Donec aliquet diam ut mi efficitur, vitae hendrerit turpis euismod. Nam nec lacus arcu. Aliquam in quam a est rhoncus maximus. Nullam euismod turpis eget urna rutrum ultricies. Morbi vestibulum sem sit amet est volutpat, id elementum neque congue.\n" +
                "\n" +
                "Fusce ullamcorper ipsum vitae consectetur efficitur. Morbi elementum justo nec quam cursus, quis ullamcorper neque accumsan. Duis ut volutpat ex. Cras fringilla diam vel enim hendrerit, eget tempus libero iaculis. Sed vitae aliquet ipsum. Phasellus non ligula in libero pretium consectetur. Nullam pellentesque consequat dui, nec suscipit eros pharetra quis. Suspendisse potenti. Vivamus sem purus, accumsan a dapibus a, fermentum eget felis. Donec auctor eros ut dui maximus, vel congue lorem vehicula. Ut malesuada orci nisi, in fermentum odio lobortis eu. Cras in leo a nisi ultricies tempor. Nullam id sapien sit amet lectus rutrum ullamcorper.\n" +
                "\n" +
                "Quisque condimentum, justo id viverra dapibus, leo risus hendrerit arcu, vel sodales risus erat vel nulla. Fusce viverra diam a libero scelerisque laoreet. Duis ut condimentum nulla. Cras laoreet nisl nec massa fermentum fermentum. Phasellus eget dui quis odio sagittis iaculis. Curabitur ac magna non ex convallis consequat. Nullam vel orci eget eros laoreet egestas. Mauris auctor tempus libero, in dignissim magna.\n" +
                "\n" +
                "Integer at nisl lorem. Fusce ac dui fermentum, laoreet odio id, ullamcorper urna. Duis mollis ante id justo consectetur, non dapibus justo vehicula. Duis pretium placerat metus, eu venenatis sapien mattis sed. Phasellus varius, justo et volutpat ultricies, ipsum turpis fermentum elit, ac elementum magna ligula nec dui. Morbi lacinia massa a lectus volutpat, ut laoreet nisl lacinia. Nullam nec ligula rutrum, faucibus metus eu, tincidunt sapien. Quisque ultricies ipsum sed orci tempor, quis efficitur magna accumsan. Cras et felis sit amet dui facilisis fringilla in non ante. Donec tincidunt convallis nunc, non placerat sem lacinia non. Proin id lorem in mauris pellentesque imperdiet. Ut euismod lacus a erat condimentum, sit amet accumsan lectus elementum. Nullam vestibulum semper nunc ac congue. Integer vel enim in dui feugiat scelerisque.\n" +
                "\n" +
                "Sed bibendum velit sed tellus varius convallis. Integer rhoncus blandit purus, sit amet sollicitudin risus mattis non. Maecenas ultrices ultricies erat nec malesuada. Sed maximus nunc a sapien varius, in feugiat nisi volutpat. Vivamus luctus euismod tellus, ac fermentum urna tempus in. Etiam tincidunt aliquam mauris, vel auctor lacus tempor nec. Ut lobortis convallis tortor in fermentum. Nulla facilisi. Sed pellentesque, eros in molestie lobortis, odio libero egestas lectus, nec dignissim sem sem eget orci.\n" +
                "\n" +
                "Morbi nec felis vel ante blandit posuere. Donec consequat leo non velit tincidunt lacinia. In vehicula, magna a posuere tincidunt, neque velit fringilla erat, non laoreet velit est ac ligula. Quisque et arcu a nisl congue tempus. Ut scelerisque id ex nec faucibus. Phasellus a ipsum nec nulla accumsan fermentum. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Nunc vitae elit sed lacus euismod feugiat. Sed ac quam eu metus scelerisque maximus. Curabitur ac nunc felis. Fusce dictum, dolor non efficitur varius, velit odio vehicula lorem, id ultricies leo magna sit amet nisl. In volutpat hendrerit libero, a viverra velit aliquet nec. Aenean et malesuada lorem.\n" +
                "\n" +
                "Fusce ut orci a lorem tristique pharetra. Pellentesque habitant\n";


        List<String> wordsArray = Arrays.asList(Article.split("\\s+"));

        for(int i = 0, chunk = 1; i < wordsArray.size() ; i+=CHUNK_SIZE, chunk++){

            int end_index = Math.min(Article.length(),i + CHUNK_SIZE);
            List<String> sublist = wordsArray.subList(i, end_index);
            String article_chunk = String.join(" ", sublist.subList(0, sublist.size()));

            jsonObject.put("ID", "12434");
            jsonObject.put("topic", "technology");
            jsonObject.put("timestamp","1-2-2024");
            jsonObject.put("chunk_text",article_chunk);
            jsonObject.put("chunk_number",chunk);
            list.add(jsonObject);

        }

        return list;
    }

    public void run(){
        while (!Thread.currentThread().isInterrupted()) {
            synchronized (this){
                try{
                    wait();
                }catch (InterruptedException e){
                    try{
                        this.close_producer();
                    }catch (SQLException ce){
                        System.out.println("Shutdown had an error");
                        return;
                    }

                }

            }
            List<ObjectNode> batch = this.test_threads();

            for(ObjectNode object:batch){
                System.out.println(object.get("chunk_text").asText());
                System.out.println();
            }

        }
    }

    public void close_producer() throws SQLException{
        kafka_producer.close();
        conn.close();
    }

}
