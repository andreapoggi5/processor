/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.imolinfo.sacmi.processor;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import java.util.LinkedHashMap;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.kafka.streams.StreamsBuilder;

/**
 *
 * @author Andrea
 */
public abstract class Streamer {
    
    protected static HttpResponse<String> registerSchema (Schema schema, String schemaRegistry, String append) throws UnirestException {
        String schemaString = schema.toString();
        
        schemaString = "{\"schema\":\"" + schemaString.replaceAll("\"", "\\\\\"").replaceAll("\t", "").replaceAll("\n", "")+ "\" }";
        HttpResponse<String> jsonResponse = Unirest.post(schemaRegistry + append)
                .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                .body(schemaString)
                .asString();
        
        return jsonResponse;
    }
    

    protected final String sourceTopic;
    protected final String destinationTopic;
    protected final String schemaFolder;
    protected final String schemaRegistryUrl;
    protected final LinkedHashMap metadata;
    protected final Properties props;
    
    protected final StreamsBuilder builder;
    protected final Parser parser;
    
    protected Streamer (String schemaRegistryUrl, String schemaFolder, LinkedHashMap metadata, String sourceTopic, String destinationTopic, Properties props) {
        props.put("default.key.serde","io.confluent.kafka.streams.serdes.avro.GenericAvroSerde");
        props.put("default.value.serde","io.confluent.kafka.streams.serdes.avro.GenericAvroSerde");
        this.sourceTopic = sourceTopic;
        this.destinationTopic = destinationTopic;
        this.props = props;
        this.metadata = metadata;
        this.schemaFolder = schemaFolder;
        this.schemaRegistryUrl = schemaRegistryUrl;
        
        this.builder = new StreamsBuilder();
        this.parser = new Schema.Parser();
    }
        
    protected abstract void stream();
    
}
