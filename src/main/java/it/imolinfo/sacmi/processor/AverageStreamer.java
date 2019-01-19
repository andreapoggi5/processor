package it.imolinfo.sacmi.processor;

import com.mashape.unirest.http.HttpResponse;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;


public class AverageStreamer extends Streamer{
    
    /**
     * Classi di utilità.
     * 
     */
    

    private class StringSerializer implements Serializer<String> {

        @Override
        public void configure(Map<String, ?> map, boolean bln) {

        }

        @Override
        public byte[] serialize(String string, String t) {
            try {
                byte[] stringSerialized = t.getBytes("UTF-8");;
                int stringSerilizedLength = stringSerialized.length;
                
                ByteBuffer buff = ByteBuffer.allocate(Integer.BYTES + stringSerilizedLength);
                buff.putInt(stringSerilizedLength);
                buff.put(stringSerialized);
                
                return buff.array();
                
            } catch (UnsupportedEncodingException ex) {
                Logger.getLogger(AverageStreamer.class.getName()).log(Level.SEVERE, null, ex);
                return null;
            }
        }
        
        public int getNumberBytes (String t) throws UnsupportedEncodingException {
            return Integer.BYTES + t.getBytes("UTF-8").length;
        }
         
        @Override
        public void close() {
       
        }
        
    }
    
    private class StringDeserializer implements Deserializer<String> {

        @Override
        public void configure(Map<String, ?> map, boolean bln) {
     
        }

        @Override
        public String deserialize(String string, byte[] bytes) {
            try {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                return deserialize(buffer);
            
            } catch (UnsupportedEncodingException ex) {
                Logger.getLogger(AverageStreamer.class.getName()).log(Level.SEVERE, null, ex);
                return null;
            }
        }
        
        private String deserialize (ByteBuffer buffer) throws UnsupportedEncodingException {
            int stringLength = buffer.getInt();
            byte[] serializedString = new byte[stringLength];
            buffer.get(serializedString);
            
            return new String (serializedString, "UTF-8");
        }
        
        @Override
        public void close() {
     
        }
        
    }
    
    private class StringListSerializer implements Serializer<List<String>> {
        @Override
        public void configure(Map<String, ?> map, boolean bln) {
        }

        @Override
        public byte[] serialize(String string, List<String> t) {
            StringSerializer stringSerializer = new StringSerializer ();
            
            int stringListSize = t.size();
            int numberBytes = this.getNumberBytes(t);
            
            ByteBuffer buffer = ByteBuffer.allocate(numberBytes);
            
            buffer.putInt(stringListSize);
            t.forEach(e -> buffer.put(stringSerializer.serialize("", e)));
            
            return buffer.array();
        }
        
        public int getNumberBytes (List<String> t) {
            StringSerializer stringSerializer = new StringSerializer ();
            int numberBytes = 0;
            
            for (String s: t)
                try {
                    numberBytes += stringSerializer.getNumberBytes(s);
                } catch (UnsupportedEncodingException ex) {
                    Logger.getLogger(AverageStreamer.class.getName()).log(Level.SEVERE, null, ex);
                }
            return Integer.BYTES + numberBytes;
        }

        @Override
        public void close() {

        }
        
        
    }




    private class StringListDeserializer implements Deserializer<List<String>> {

        @Override
        public void configure(Map<String, ?> map, boolean bln) {
        }

        @Override
        public List<String> deserialize(String string, byte[] bytes) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            return deserialize(buffer);
        }
        
        public List<String> deserialize(ByteBuffer buffer) {
            try {
                StringDeserializer stringDeserializer = new StringDeserializer();
                
                List<String> stringList = new ArrayList<>();
                int listSize = buffer.getInt();
                
                for (int i = 0; i < listSize; i ++)
                    stringList.add(stringDeserializer.deserialize(buffer));
                
                return stringList;
            } catch (UnsupportedEncodingException ex) {
                Logger.getLogger(AverageStreamer.class.getName()).log(Level.SEVERE, null, ex);
                return null;
            }
        }
        
        @Override
        public void close() {
        }
        
    }
    
    private class StringListSerde implements Serde<List<String>> {

        @Override
        public void configure(Map<String, ?> map, boolean bln) {

        }

        @Override
        public void close() {

        }

        @Override
        public Serializer<List<String>> serializer() {
            return new StringListSerializer();
        }

        @Override
        public Deserializer<List<String>> deserializer() {
            return new StringListDeserializer();
        }
        
    }
    
    
    
    
    private class AverageRecord {

        private String timestamp;
        private List<Variable> variableList;

        public AverageRecord (String timestamp, List<Variable> variableList) {
            this.timestamp = timestamp;
            this.variableList = variableList;
        }
        
        @Override
        public String toString() {
            return "timestamp: " + this.timestamp + " values: " + variableList;
        }
        
    }
    
    private class AverageRecordSerializer implements Serializer<AverageRecord> {
        
        @Override
        public void configure(Map<String, ?> map, boolean bln) {
        }

        @Override
        public byte[] serialize(String string, AverageRecord t) {
            try {
                StringSerializer stringSerializer = new StringSerializer();
                int timestampBytes = stringSerializer.getNumberBytes(t.timestamp);
                byte[] timestampSerialized = stringSerializer.serialize("", t.timestamp);

                VariableListSerializer variableListSerializer = new VariableListSerializer ();
                int listBytes = variableListSerializer.getNumberBytes(t.variableList);
                byte[] variableListSerialized = variableListSerializer.serialize("", t.variableList);
                        
                ByteBuffer buffer = ByteBuffer.allocate(timestampBytes + listBytes);
                buffer.put(timestampSerialized);
                buffer.put(variableListSerialized);
                
                return buffer.array();
                
            } catch (UnsupportedEncodingException ex) {
                Logger.getLogger(AverageStreamer.class.getName()).log(Level.SEVERE, null, ex);
                return null;
            }
        }
        
        @Override
        public void close() {
            
        }
        
    }
    
    private class AverageRecordDeserializer implements Deserializer<AverageRecord> {

        @Override
        public void configure(Map<String, ?> map, boolean bln) {
        }
        
        @Override
        public AverageRecord deserialize(String string, byte[] bytes) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            return deserialize(buffer);
        }
        
        public AverageRecord deserialize(ByteBuffer buffer) {
            try {
                StringDeserializer stringDeserializer = new StringDeserializer();
                VariableListDeserializer variableListDeserializer = new VariableListDeserializer();
                
                String timestamp = stringDeserializer.deserialize(buffer);
                List<Variable> variableList = variableListDeserializer.deserialize(buffer);
                
                return new AverageRecord (timestamp, variableList);
            }
            catch (UnsupportedEncodingException ex) {
                Logger.getLogger(AverageStreamer.class.getName()).log(Level.SEVERE, null, ex);
                return null;
                
            }
        }
        
        @Override
        public void close() {
            
        }
    }

    private class AverageRecordSerde implements Serde<AverageRecord> {

        @Override
        public void configure(Map<String, ?> map, boolean bln) {
        }

        @Override
        public void close() {
        }

        @Override
        public Serializer<AverageRecord> serializer() {
            return new AverageRecordSerializer();
        }

        @Override
        public Deserializer<AverageRecord> deserializer() {
            return new AverageRecordDeserializer();
        }
        
    }
    



    private class Variable {
        private String name;
        private String type;
        private String unit;
        private long occ;
        private long sum;

        public Variable (String name, String type, String unit, long occ, long sum) {
            this.name = name;
            this.type = type;
            this.unit = unit;
            this.occ = occ;
            this.sum = sum;
        }

        public double getAverage () {
            return (double)this.sum / this.occ;
        }

    }

    private class VariableSerializer implements Serializer<Variable> {

        @Override
        public void configure(Map<String, ?> map, boolean bln) {
        }

        @Override
        public byte[] serialize(String string, Variable t) {
            try {
                StringSerializer stringSerializer = new StringSerializer();

                int nameBytes = stringSerializer.getNumberBytes(t.name);
                byte[] nameSerialized = stringSerializer.serialize("", t.name);

                int typeBytes = stringSerializer.getNumberBytes(t.type);
                byte[] typeSerialized = stringSerializer.serialize("", t.type);

                int unitBytes = stringSerializer.getNumberBytes(t.unit);
                byte[] unitSerialized = stringSerializer.serialize("", t.unit);


                ByteBuffer buffer = ByteBuffer.allocate(nameBytes + typeBytes + unitBytes + Long.BYTES + Long.BYTES);
                buffer.put(nameSerialized);
                buffer.put(typeSerialized);
                buffer.put(unitSerialized);
                buffer.putLong(t.occ);
                buffer.putLong(t.sum);

                return buffer.array();

            } catch (UnsupportedEncodingException ex) {
                Logger.getLogger(AverageStreamer.class.getName()).log(Level.SEVERE, null, ex);
                return null;
            }
        }

        public int getNumberBytes (Variable v) throws UnsupportedEncodingException {
            StringSerializer stringSerializer = new StringSerializer();

            int nameBytes = stringSerializer.getNumberBytes(v.name);
            int typeBytes = stringSerializer.getNumberBytes(v.type);
            int unitBytes = stringSerializer.getNumberBytes(v.unit);

            return nameBytes + typeBytes + unitBytes + Long.BYTES + Long.BYTES;

        }

        @Override
        public void close() {
        }

    }

    private class VariableDeserializer implements Deserializer<Variable> {

        @Override
        public void configure(Map<String, ?> map, boolean bln) {

        }

        @Override
        public Variable deserialize(String string, byte[] bytes) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            return deserialize(buffer);
        }

        private Variable deserialize (ByteBuffer buff) {
            try {
                StringDeserializer stringDeserializer = new StringDeserializer();
                String name = stringDeserializer.deserialize(buff);
                String type = stringDeserializer.deserialize(buff);
                String unit = stringDeserializer.deserialize(buff);

                long occ = buff.getLong();
                long sum = buff.getLong();

                return new Variable (name, type, unit, occ, sum);
            } catch (UnsupportedEncodingException ex) {
                Logger.getLogger(AverageStreamer.class.getName()).log(Level.SEVERE, null, ex);
                return null;
            }
        }

        @Override
        public void close() {

        }

    }

    private class VariableListSerializer implements Serializer<List<Variable>> {

        @Override
        public void configure(Map<String, ?> map, boolean bln) {

        }

        @Override
        public byte[] serialize(String string, List<Variable> t) {
          VariableSerializer variableSerializer = new VariableSerializer();
          int variableListSize = t.size();

          int numberBytes = getNumberBytes(t);

          ByteBuffer buffer = ByteBuffer.allocate(numberBytes);

          buffer.putInt(variableListSize);
          t.forEach(e -> buffer.put(variableSerializer.serialize("", e)));

          return buffer.array();
        }

        public int getNumberBytes (List<Variable> t) {
            VariableSerializer variableSerializer = new VariableSerializer ();
            int numberBytes = 0;

            for (Variable s: t)
                try {
                    numberBytes += variableSerializer.getNumberBytes(s);
                } catch (UnsupportedEncodingException ex) {
                    Logger.getLogger(AverageStreamer.class.getName()).log(Level.SEVERE, null, ex);
                }
            return Integer.BYTES+ numberBytes;
        }

        @Override
        public void close() {

        }

    }

    private class VariableListDeserializer implements Deserializer<List<Variable>> {

        @Override
        public void configure(Map<String, ?> map, boolean bln) {

        }

        @Override
        public List<Variable> deserialize(String string, byte[] bytes) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            return deserialize(buffer);
        }

        public List<Variable> deserialize(ByteBuffer buffer) {
            VariableDeserializer variableDeserializer = new VariableDeserializer();

            List<Variable> variableList = new ArrayList<>();
            int listSize = buffer.getInt();

            for (int i = 0; i < listSize; i ++)
                variableList.add(variableDeserializer.deserialize(buffer));

            return variableList;

        }

        @Override
        public void close() {

        }

    }


    private final List<String> variables;
    private final Schema[] sourceTopicSchemas;
    
    public AverageStreamer (String schemaRegistryUrl, String schemaFolder, LinkedHashMap metadata, String topicSrc, String topicDest, List<String> variables, Properties props, Schema[] sourceTopicSchemas) {
        super(schemaRegistryUrl, schemaFolder, metadata, topicSrc, topicDest, props);
        this.variables = variables;
        this.sourceTopicSchemas = sourceTopicSchemas;
    }
    
    private GenericRecord getKeyRecord (Schema keySchema, List<String> values) {
        GenericRecordBuilder keyRecordBuilder = new GenericRecordBuilder(keySchema);

        keyRecordBuilder.set("keys", values);
        GenericRecord keyRecord = keyRecordBuilder.build();

        return keyRecord;
    }
    
    private GenericRecord getValueRecord (Schema valueSchema, AverageRecord value) {
        GenericRecordBuilder valueRecordBuilder = new GenericRecordBuilder(valueSchema);
        ArrayList<String> messageData;
        if (this.metadata != null) {
            this.metadata.forEach((k, v) -> {
                if (!((String)k).equals("message"))
                    valueRecordBuilder.set((String)k, v);
            });
        }

        valueRecordBuilder.set("timestamp", value.timestamp);
        Schema payloadSchema = valueSchema.getField("payload").schema();
        GenericRecord payload = new GenericData.Record(payloadSchema);
        List<GenericRecord> array = new ArrayList<>();

        for (int i = 0; i < value.variableList.size(); i ++) {
            Schema variableSchema = payloadSchema.getField(value.variableList.get(i).name).schema();
            GenericRecord gr = new GenericData.Record(variableSchema);
            gr.put("value", value.variableList.get(i).getAverage());
            gr.put("unit", value.variableList.get(i).unit);
            payload.put(value.variableList.get(i).name, gr);
        }

        valueRecordBuilder.set("payload", payload);
        GenericRecord valueRecord = valueRecordBuilder.build();

        return valueRecord;
    }

    private GenericRecord getValueRecord2 (Schema valueSchema, AverageRecord value) {
        GenericRecordBuilder valueRecordBuilder = new GenericRecordBuilder(valueSchema);
        ArrayList<String> messageData;
        if (this.metadata != null) {
            this.metadata.forEach((k, v) -> {
                if (!((String)k).equals("message"))
                    valueRecordBuilder.set((String)k, v);
            });
        }

        valueRecordBuilder.set("timestamp", value.timestamp);
        Schema childSchema = valueSchema.getField("payload").schema().getElementType();
        List<GenericRecord> array = new ArrayList<>();

        value.variableList.forEach(e ->  {
            GenericRecord gr = new GenericData.Record(childSchema);
            gr.put("name", e.name);
            gr.put("unit", e.unit);
            gr.put("value", e.getAverage());
            array.add(gr);
        });


        valueRecordBuilder.set("payload", array);
        GenericRecord valueRecord = valueRecordBuilder.build();

        return valueRecord;
    }

    private Schema getKeySchema (String nameSchema, int numVariables) throws IOException {
        File folder;
        if (this.schemaFolder != null)
            folder = new File (this.schemaFolder);
        else
            folder = new File ("./avroSchemas");
        
        if (!folder.exists()) {
            folder.mkdir();
        }
        
        String name = nameSchema.substring(0, nameSchema.lastIndexOf('.')).replaceAll("-", "_");
        File keySchemaFile = new File ("./avroSchemas/" + nameSchema);
        String str = "{\n\t\"type\": \"record\",\n\t\"name\": \"" + name + "\",\n\t\"namespace\": \"it.imolinfo.sacmi.processor\",\n\t\"fields\": [\n" +
                "\t\t\t{\n\t\t\t\t\"name\": \"keys\", \"type\": {\"type\": \"array\", \"items\": \"string\"}\n\t\t\t}\n";

        str = str + "\t]\n}";
        
        try (BufferedWriter buff = new BufferedWriter (new OutputStreamWriter (new FileOutputStream(keySchemaFile)))) {
            buff.write(str);
        }
        
        Schema keySchema = this.parser.parse(keySchemaFile);
        return keySchema;
    }
    
    private Schema getValueSchema (String nameSchema) throws IOException {
        File folder = new File ("./avroSchemas");
        if (!folder.exists()) {
            folder.mkdir();
        }
        

        String name = nameSchema.substring(0, nameSchema.lastIndexOf('.')).replaceAll("-", "_");
        File valueSchemaFile = new File ("./avroSchemas/" + nameSchema);
        String str = "{\n\t\"type\": \"record\",\n\t\"name\": \"" + name + "\",\n\t\"namespace\": \"it.imolinfo.sacmi.processor\",\n\t\"fields\": [\n";
        if (this.metadata != null) {
            Set<String> keySet = this.metadata.keySet();
            for (String key : keySet)
                if (!key.equals("message"))
                    str = str + "\t\t{\n\t\t\t\"name\": \"" + key + "\",\n\t\t\t\"type\": \"string\"\n\t\t},\n";
                else {
                    ArrayList<String> messageData = (ArrayList<String>) this.metadata.get("message");
                    if (messageData != null)
                        for (String field : messageData) {
                            String type = sourceTopicSchemas[1].getField(field).schema().getType().getName();
                            str = str + "\t\t{\n\t\t\t\"name\": \"" + field + "\",\n\t\t\t\"type\": \"" + type + "\"\n\t\t},\n";
                        }

                }
        }
        
        str = str + "\t\t{\n\t\t\t\"name\": \"timestamp\",\n\t\t\t\"type\": \"string\",\n\t\t\t\"logicalType\": \"timestamp-micros\"\n\t\t},\n"  +
                "\t\t{\n\t\t\t\"name\": \"payload\",\n\t\t\t\"type\": {\"type\": \"array\", \"items\": {\"type\": \"record\", \"name\": \"variable\", \"fields\" : [\n" +
                "\t\t\t\t{\n\t\t\t\t\t\"name\": \"name\", \"type\": \"string\"\n\t\t\t\t},\n" +
                "\t\t\t\t{\n\t\t\t\t\t\"name\": \"unit\", \"type\": \"string\"\n\t\t\t\t},\n" +
                "\t\t\t\t{\n\t\t\t\t\t\"name\": \"value\",\"type\": \"double\"\n\t\t\t\t}\n" +
                "\t\t\t]}\n\t\t\t}\n\t\t}\n\t]\n}";
        
        try (BufferedWriter buff = new BufferedWriter (new OutputStreamWriter (new FileOutputStream(valueSchemaFile)))) {
            buff.write(str);
        }

        Schema valueSchema = this.parser.parse(str);
        return valueSchema;
    }

    private Schema getValueSchema2 (String nameSchema) throws IOException {
        File folder = new File ("./avroSchemas");
        if (!folder.exists()) {
            folder.mkdir();
        }


        String name = nameSchema.substring(0, nameSchema.lastIndexOf('.')).replaceAll("-", "_");
        File valueSchemaFile = new File ("./avroSchemas/" + nameSchema);
        String str = "{\n\t\"type\": \"record\",\n\t\"name\": \"" + name + "\",\n\t\"namespace\": \"it.imolinfo.sacmi.processor\",\n\t\"fields\": [\n";
        if (this.metadata != null) {
            Set<String> keySet = this.metadata.keySet();
            for (String key : keySet)
                if (!key.equals("message"))
                    str = str + "\t\t{\n\t\t\t\"name\": \"" + key + "\",\n\t\t\t\"type\": \"string\"\n\t\t},\n";
                else {
                    ArrayList<String> messageData = (ArrayList<String>) this.metadata.get("message");
                    if (messageData != null)
                        for (String field : messageData) {
                            String type = sourceTopicSchemas[1].getField(field).schema().getType().getName();
                            str = str + "\t\t{\n\t\t\t\"name\": \"" + field + "\",\n\t\t\t\"type\": \"" + type + "\"\n\t\t},\n";
                        }

                }
        }

        str = str + "\t\t{\n\t\t\t\"name\": \"timestamp\",\n\t\t\t\"type\": \"string\",\n\t\t\t\"logicalType\": \"timestamp-micros\"\n\t\t},\n"  +
                "\t\t{\n\t\t\t\"name\": \"payload\",\n\t\t\t\"type\": {\n\t\t\t\t\"type\": \"record\", \"name\": \"average_payload\", \"fields\" : [\n";
        for (int i = 0; i < this.variables.size(); i ++) {
            str = str + "\t\t\t\t\t{\n\t\t\t\t\t\t\"name\": \"" + this.variables.get(i) + "\", \n\t\t\t\t\t\t\"type\": {\n\t\t\t\t\t\t\t\"type\": \"record\", \n\t\t\t\t\t\t\t\"name\": \"" + this.variables.get(i) + "_data\", \n\t\t\t\t\t\t\t\"fields\": [\n";
            str = str + "\t\t\t\t\t\t\t\t{\n\t\t\t\t\t\t\t\t\t\"name\": \"value\", \n\t\t\t\t\t\t\t\t\t\"type\": \"double\"\n\t\t\t\t\t\t\t\t},\n";
            str = str + "\t\t\t\t\t\t\t\t{\n\t\t\t\t\t\t\t\t\t\"name\": \"unit\", \"type\": \"string\"\n\t\t\t\t\t\t\t\t}\n\t\t\t\t\t\t\t]\n\t\t\t\t\t\t}\n\t\t\t\t\t}";
            if (i != this.variables.size() - 1)
                    str = str + ",";
            str = str + "\n";
        }

        str = str + "\t\t\t\t]\n\t\t\t\t}\n\t\t\t}\n\t\t]\n}";

        try (BufferedWriter buff = new BufferedWriter (new OutputStreamWriter (new FileOutputStream(valueSchemaFile)))) {
            buff.write(str);
        }

        Schema valueSchema = this.parser.parse(str);
        return valueSchema;
    }
    
    private void average (Schema keySchema, Schema valueSchema)  {
        try {
            /**
             * Creo il serde per il GenericAvro
             */
            Serde<GenericRecord> genericAvroSerde = new GenericAvroSerde();
            boolean isKeySerde = false;
            genericAvroSerde.configure(
                    Collections.singletonMap(
                            AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                            this.schemaRegistryUrl),
                    isKeySerde);


            /**
             * Creo lo stream che calcola la media.
             */
            KStream<GenericRecord, GenericRecord> source =
                    builder.stream(this.sourceTopic);

            KStream <GenericRecord, GenericRecord> average = source
                    .map((key, value) ->
                            KeyValue.pair(this.variables, value))
                    .groupByKey(
                            Serialized.with(new StringListSerde(), genericAvroSerde))
                    .aggregate(
                            () -> {
                                List<Variable> variableList = new ArrayList<>();
                                this.variables.forEach(e -> variableList.add(new Variable(e, null, null, 0,  0)));
                                return new AverageRecord (null, variableList);
                            },
                            (aggKey, newValue, aggValue) ->  {
                                if (aggValue.timestamp == null) {
                                    /**
                                     * Setto l'AverageRecord
                                     */
                                    for (int i = 0; i < aggKey.size(); i ++) {
                                        String name = aggKey.get(i);
                                        Variable v = aggValue.variableList.get(i);
                                        v.name = name;
                                        GenericRecord mapVariable = (GenericRecord)((GenericRecord)newValue.get("payload")).get(name);
                                        v.type = "" + mapVariable.getSchema().getField("value").schema().getType();
                                        v.unit = "" + mapVariable.get("unit");
                                    }

                                }

                                /**
                                 * - setto metadati
                                 */
                                ArrayList<String> list = (ArrayList<String>) metadata.get("message");
                                for (String field : list)
                                    metadata.put(field, newValue.get(field));

                                aggValue.timestamp = "" + newValue.get("timestamp");
                                for (int i = 0; i < aggValue.variableList.size(); i ++) {
                                    Variable v = aggValue.variableList.get(i);
                                    v.occ ++;
                                    switch (v.type) {
                                        case "INT":
                                            v.sum += Integer.parseInt("" + ((GenericRecord)((GenericRecord)newValue.get("payload")).get(v.name)).get("value"));
                                            break;
                                        case "FLOAT":
                                            v.sum += Float.parseFloat("" + ((GenericRecord)((GenericRecord)newValue.get("payload")).get(v.name)).get("value"));
                                            break;
                                        case "DOUBLE":
                                            v.sum += Double.parseDouble("" + ((GenericRecord)((GenericRecord)newValue.get("payload")).get(v.name)).get("value"));
                                            break;
                                        default:
                                            System.out.println("tipo non supportato");
                                            break;
                                    }
                                }
                                return aggValue;
                            },
                            Materialized.with(new StringListSerde(), new AverageRecordSerde()))
                    .toStream()
                    .map((key, value) -> KeyValue.pair(getKeyRecord(keySchema, key), getValueRecord(valueSchema, value)));

            average.foreach((key, value) -> System.out.println("chiave = " + key + " valore = " + value));
            average.to(this.destinationTopic);

            KafkaStreams stream = new KafkaStreams(builder.build(), props);
            stream.start();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    public void stream() {
        /**
         * Creo e registro gli schemi presso lo schema registry.
         */
        try {
            Schema keySchema = getKeySchema("keyschema_" + this.destinationTopic + "_topic.avsc", this.variables.size());

            Schema valueSchema = getValueSchema2("valueschema_" + this.destinationTopic + "_topic.avsc");

            HttpResponse<String> res;

            res = registerSchema(keySchema, this.schemaRegistryUrl, "/subjects/" + this.destinationTopic + "-key/versions");
            if (res.getStatus() == 200)
                System.out.println("Schema registered: \n200 ok");
            else
                System.out.println("Schema not registered: \n" + res.getStatus() + "\n" + res.getBody());


            res = registerSchema(valueSchema, this.schemaRegistryUrl, "/subjects/" + this.destinationTopic + "-value/versions");
            if (res.getStatus() == 200)
                System.out.println("Schema registered: \n200 ok");
            else
                System.out.println("Schema not registered: \n" + res.getStatus() + "\n" + res.getBody());

            average(keySchema, valueSchema);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }
    
}
