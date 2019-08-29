package cn.sparticles.service.bolt;

import cn.sparticles.utils.RedisUtil;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DayPVViewsBolt extends BaseBasicBolt {

    private List<String> list = Lists.newArrayList();

    private String redisKey = "PVDayCount";
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (!input.getSourceComponent().equalsIgnoreCase(Constants.SYSTEM_COMPONENT_ID)) {  // 如果收到非系统级别的tuple，统计信息到局部变量mids
            String logStr = input.getStringByField("logStr");
            list.add(logStr);
        }
        else {
            RedisUtil redisUtil = RedisUtil.getInstance();
            String count = redisUtil.get(redisKey);
            if(StringUtils.isBlank(count)){
                count = "0";
            }
            redisUtil.set(redisKey,String.valueOf(Long.valueOf(count) + list.size()));
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
    }

    /**
     * 定时任务进行执行
     * @return
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> config = new HashMap<>();
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
        return config;
    }
}
