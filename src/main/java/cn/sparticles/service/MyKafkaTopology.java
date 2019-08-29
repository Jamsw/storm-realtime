package cn.sparticles.service;

import cn.sparticles.service.bolt.DayPVViewsBolt;
import cn.sparticles.service.bolt.PVBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

public class MyKafkaTopology  {

    public static void main(String[] args) throws Exception {
        KafkaSpoutConfig.Builder<String,String> builder = KafkaSpoutConfig.builder("127.0.0.1:9092","user_tests");
        builder.setProp("group.id","testStorm");
        KafkaSpoutConfig<String, String> kafkaSpoutConfig= builder.build();
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("WordCountFileSpout",new KafkaSpout(kafkaSpoutConfig), 4);
        topologyBuilder.setBolt("PVCount",new PVBolt()).shuffleGrouping("WordCountFileSpout");
        topologyBuilder.setBolt("RedisPVCache",new DayPVViewsBolt()).shuffleGrouping("PVCount");
        Config config = new Config();
        if(args !=null && args.length > 0){
            config.setDebug(false);
            StormSubmitter submitter= new StormSubmitter();
            submitter.submitTopology("kafkaStromTopo", config, topologyBuilder.createTopology());
        }else{
            config.setDebug(true);
            LocalCluster cluster= new LocalCluster();
            cluster.submitTopology("kafkaStromTopo", config, topologyBuilder.createTopology());
        }

    }
}
