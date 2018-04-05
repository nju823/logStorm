
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class logCheckBolt extends BaseRichBolt {

    private OutputCollector collector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        System.out.println("normalizerBolt-prepare!");
    }

    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        //System.out.println(word);
        //todo check
        collector.emit(tuple, new Values(word));
    }

    public void cleanup() {
        System.out.println("normalizerBolt-clean!");
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("logCheck"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
