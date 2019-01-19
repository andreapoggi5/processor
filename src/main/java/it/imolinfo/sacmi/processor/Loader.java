package it.imolinfo.sacmi.processor;

import com.esotericsoftware.yamlbeans.YamlReader;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.avro.Schema;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Loader {
    
    private List<Map> getYamlInstruction(File config) throws FileNotFoundException, IOException {
        List<Map> result = new ArrayList<>();
        YamlReader reader = new YamlReader(new FileReader(config));
        Map entry = (Map) reader.read();
        while (entry != null) {
            result.add(entry);
            entry = (Map) reader.read();
        }
        return result;
    }
    
    private Properties getProperties (LinkedHashMap lm) {
        Properties props = new Properties();
        lm.keySet().stream()
                .forEach(e -> props.put(e, lm.get(e)));
        return props;
    }

    private Schema[] getSchemas (String schemaRegistryUrl, String topic) {
        try {
            Schema[] schemas = new Schema[2];

            HttpResponse<String> response = Unirest.get(schemaRegistryUrl + "/subjects/" + topic + "-key/versions/1").asString();
            String body = response.getBody();
            body = body.substring(body.indexOf("schema") + 9, body.length() - 2);
            body = body.replace("\\", "");

            Schema keySchema = new Schema.Parser().parse(body);
            schemas[0] = keySchema;

            response = Unirest.get(schemaRegistryUrl + "/subjects/" + topic + "-value/versions/1").asString();
            body = response.getBody();
            body = body.substring(body.indexOf("schema") + 9, body.length() - 2);
            body = body.replace("\\", "");

            Schema valueSchema = new Schema.Parser().parse(body);
            schemas[1] = valueSchema;

            return schemas;

        } catch (UnirestException e) {
            e.printStackTrace();
            return null;
        }

    }
    private void loadAverageStreamer (Map m, String schemaRegistryUrl){
        List<String> list = (List<String>)m.get("variables");
        /*List<String[]> variables = new ArrayList<> ();
        list.forEach(e -> {
                    variables.add(e.split(":"));
                });
        */
        LinkedHashMap metadata = (LinkedHashMap)m.get("metadata");
        LinkedHashMap configuration = (LinkedHashMap)m.get("configuration");
        Properties props = getProperties(configuration);
        Schema[] sourceTopicSchemas = getSchemas(schemaRegistryUrl, (String)m.get("sourceTopic"));
        AverageStreamer s = new AverageStreamer (schemaRegistryUrl, (String)m.get("schemaFolder"), metadata, (String)m.get("sourceTopic"),(String) m.get("destinationTopic"), list, props, sourceTopicSchemas);
        s.stream();
    }
    
    private void loadIsolateStreamer (Map m, String schemaRegistryUrl) throws IOException {
        List<String> list = (List<String>)m.get("variables");
        List<String[]> variables = new ArrayList<> ();
        list.forEach(e -> {
                    variables.add(e.split(":"));
                });
        
        LinkedHashMap metadata = (LinkedHashMap)m.get("metadata");
        LinkedHashMap configuration = (LinkedHashMap)m.get("configuration");
        LinkedHashMap when = (LinkedHashMap)m.get("when");
        Properties props = getProperties (configuration);
        
        IsolateStreamer f = new IsolateStreamer (schemaRegistryUrl, (String)m.get("schemaFolder"), metadata, (String)m.get("sourceTopic"),(String) m.get("destinationTopic"), variables, when, props); //evalList);
        f.stream();

        System.out.println ("Filter: Ancora da implementare");
    }
    
    private void loadDeleteStreamer (Map m){
                System.out.println ("Delete: Ancora da implementare");
//        String str = (String)m.get("variable");
//        String[] splitted = str.split(":");
//        LinkedHashMap lm = (LinkedHashMap)m.get("config");
//        Properties prop = new Properties();
//        lm.keySet().stream()
//                .forEach(e -> prop.put(e, lm.get(e)));
//        Streamer s = new Streamer ((String)m.get("type"), (String)m.get("topicSrc"),(String) m.get("topicDest"), splitted[0], splitted[1], prop, schemaFolderPath);
//        s.run();
    }
    
    private void loadMinStreamer (Map m ){
        System.out.println ("Min: Ancora da implementare");
//        String str = (String)m.get("variable");
//        String[] splitted = str.split(":");
//        LinkedHashMap lm = (LinkedHashMap)m.get("config");
//        Properties prop = new Properties();
//        lm.keySet().stream()
//                .forEach(e -> prop.put(e, lm.get(e)));
//        Streamer s = new Streamer ((String)m.get("type"), (String)m.get("topicSrc"),(String) m.get("topicDest"), splitted[0], splitted[1], prop, schemaFolderPath);
//        s.run();
    }
    
    private void loadMaxStreamer (Map m){
        System.out.println ("Max: Ancora da implementare");
//        String str = (String)m.get("variable");
//        String[] splitted = str.split(":");
//        LinkedHashMap lm = (LinkedHashMap)m.get("config");
//        Properties prop = new Properties();
//        lm.keySet().stream()
//                .forEach(e -> prop.put(e, lm.get(e)));
//        Streamer s = new Streamer ((String)m.get("type"), (String)m.get("topicSrc"),(String) m.get("topicDest"), splitted[0], splitted[1], prop, schemaFolderPath);
//        s.run();
    }
    
    public static void main(String[] args) throws IOException, FileNotFoundException, UnirestException {
        if (args.length != 2) {
            System.out.println("Usage: java -jar processor-1.0-SNAPSHOT-jar-with-dependencies.jar /path/to/configFile schemaRegistryUrl");
            System.exit(-1);
        }
        
        
        Loader loader = new Loader();
        System.out.println("Load istruction ...");
        List <Map> l = loader.getYamlInstruction(new File (args[0]));
        System.out.println("Istruction loaded");

        l.forEach(e -> {
                    System.out.println("Carico Streamer " + e);
                    String type = (String)e.get("type");
                    switch (type) {
                        case "average":
                            loader.loadAverageStreamer(e, args[1]);
                            break;
                        case "isolate":
                            try {
                                loader.loadIsolateStreamer(e, args[1]);
                            } catch (IOException ex) {
                                Logger.getLogger(Loader.class.getName()).log(Level.SEVERE, null, ex);
                            }
                            break;
                        case "delete":
                            loader.loadDeleteStreamer(e);
                            break;
                        case "min":
                            loader.loadMinStreamer(e);
                            break;
                        case "max":
                            loader.loadMaxStreamer(e);
                            break;
                        default:
                            System.out.println("type " + type + " not defined");
                            break;
                    }
                });
    }
}
