package cn.sparticles.service.bolt;

import cn.sparticles.model.LabelCommonModel;
import cn.sparticles.utils.FastJsonUtils;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class PVBolt extends BaseBasicBolt {

    /**
     * 进行没10秒一次聚合，聚合完成传递下一个bolt进行redis存储持久化处理
     * @param input
     * @param collector
     */
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        //进行格式化数据处理
        String kafkaTopicMes = (String) input.getValue(4);
        LabelCommonModel labelCommonModel = FastJsonUtils.toBean(kafkaTopicMes, LabelCommonModel.class);
        collector.emit(new Values(labelCommonModel.getLogStr()));
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("logStr"));
    }


}
