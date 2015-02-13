package com.storm.demo;

/**
 * Created by David on 10/02/2015.
 */

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import org.elasticsearch.storm.EsBolt;

public class TwitterDemoTopology {
    public static void main(String[] args) throws IOException {
        String consumerKey = args[0];
        String consumerSecret = args[1];
        String accessToken = args[2];
        String accessTokenSecret = args[3];
        String[] arguments = args.clone();
        String[] keyWords = Arrays.copyOfRange(arguments, 4, arguments.length);

        Map esConf = new HashMap();
        esConf.put("es.index.auto.create", "yes");
        esConf.put("es.input.json", "true");
        esConf.put("es.batch.size.entries", "100");
        //esConf.put("es.mapping.id", "id_str");
        esConf.put("es.mapping.timestamp", "timestamp_ms");

        TopologyBuilder builder = new TopologyBuilder();

        //builder.setSpout("twitter", new StringSpout());
        builder.setSpout("twitter", new TwitterSampleSpout(consumerKey, consumerSecret, accessToken, accessTokenSecret, keyWords));
        builder.setBolt("print", new PrinterBolt()).shuffleGrouping("twitter");
        //builder.setBolt("filter", new LocationFilterBolt()).shuffleGrouping("twitter");
        //builder.setBolt("filter", new HashtagFilterBolt()).shuffleGrouping("twitter");
        builder.setBolt("es-bolt", new EsBolt("twitter/tweet", esConf), 5)
                .shuffleGrouping("twitter" /*"filter"*/)
                .addConfiguration(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 2);

        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("test", conf, builder.createTopology());

        //Utils.sleep(100000);
        System.in.read();
        cluster.shutdown();
    }
}