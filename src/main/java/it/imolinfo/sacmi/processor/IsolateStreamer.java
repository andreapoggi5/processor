/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.imolinfo.sacmi.processor;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author Andrea
 */
public class IsolateStreamer extends Streamer {

    private final List<String[]> variables;
    private final LinkedHashMap when;
            
    public IsolateStreamer(String schemaRegistryUrl, String schemaFolder, LinkedHashMap metadata, String sourceTopic, String destinationTopic, List<String[]> variables, LinkedHashMap when, Properties props) {
        super(schemaRegistryUrl, schemaFolder, metadata, sourceTopic, destinationTopic, props);
        this. variables = variables;
        this.when = when;
    }

        
    private void isolate () {
        
    }
            
    @Override
    protected void stream() {
        isolate();
        
    }
    
    
    
}
