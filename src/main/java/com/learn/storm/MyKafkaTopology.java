package com.learn.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.LocalAssignment;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.spout.Scheme;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import storm.kafka.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hello world!
 *
 */
public class MyKafkaTopology
{

    public static class KafkaWordSplitter extends BaseRichBolt{

        private static final Log LOG = LogFactory.getLog(KafkaWordSplitter.class);
        private static final long serialVersionUID = 886149197481637894L;
        private OutputCollector collector;
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

            this.collector = outputCollector;
        }

        public void execute(Tuple tuple) {
            String line = tuple.getString(0);
            LOG.info("RECV[kafka -> spliter] " + line);
            String[] words = line.split("\\s+");
            for(String word : words){
                LOG.info("EMIT[splitter -> counter] " + word);
                collector.emit(tuple, new Values(word, 1));
            }
            collector.ack(tuple);
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word", "count"));
        }
    }

    public static class WordCounter extends BaseRichBolt{

        private static final Log LOG = LogFactory.getLog(WordCounter.class);
        private static final long serialVersionUID = 886149197481637894L;
        private OutputCollector collector;
        private Map<String, AtomicInteger> counterMap;
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
            this.counterMap = new HashMap<String, AtomicInteger>();
        }

        public void execute(Tuple tuple) {
            String word = tuple.getString(0);
            int count = tuple.getInteger(1);
            LOG.info("RECV[spliter -> counter] " + word + " : " + count);
            AtomicInteger ai = this.counterMap.get(word);
            if(ai == null){
                ai = new AtomicInteger();
                this.counterMap.put(word, ai);
            }
            ai.addAndGet(count);
            collector.ack(tuple);
            LOG.info("CHECK statistics map: " + this.counterMap);
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word", "count"));
        }

        public void cleanup(){
            LOG.info("The final result:");
            Iterator<Map.Entry<String, AtomicInteger>> iter = this.counterMap.entrySet().iterator();
            while(iter.hasNext()){
                Map.Entry<String, AtomicInteger> entry = iter.next();
                LOG.info(entry.getKey() + "\t:\t" + entry.getValue().get());
            }
        }
    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException,
        AuthorizationException
    {
        String zks = "10.108.219.4:2181,10.108.219.208:2181,10.108.219.12:2181";
        String topic = "my-replic";
        String zkRoot = "/storm";
        String id = "word";

        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
//        spoutConfig.
        spoutConfig.zkServers = Arrays.asList(new String[]{"10.108.219.4", "10.108.219.208", "10.108.219.12"});

        spoutConfig.zkPort = 2181;

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-reader", new KafkaSpout(spoutConfig), 5);

        builder.setBolt("word-splitter", new KafkaWordSplitter(), 2).shuffleGrouping("kafka-reader");

        builder.setBolt("word-count", new WordCounter()).fieldsGrouping("word-splitter", new Fields("word"));

        Config config = new Config();

        String name = MyKafkaTopology.class.getSimpleName();
        if(args != null && args.length > 0){
            config.put(Config.NIMBUS_HOST, args[0]);
            config.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(name, config, builder.createTopology());

        }else{
            config.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(name, config, builder.createTopology());
            Thread.sleep(60000);
            cluster.shutdown();
        }
    }
}
