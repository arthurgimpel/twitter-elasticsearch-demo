package com.storm.demo;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.json.DataObjectFactory;

import java.util.Map;

public class HashtagFilterBolt extends BaseBasicBolt {

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        //System.out.println(tuple);
        String json = tuple.getString(0);
        Status status = null;
        try {
            status = DataObjectFactory.createStatus(json);
        } catch (TwitterException e) {
            //e.printStackTrace();
        }
        if(status != null && status.getHashtagEntities() != null && status.getHashtagEntities().length > 0)
            collector.emit(new Values(json));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("json"));
    }

    @Override
    public void cleanup() {

    }
}
