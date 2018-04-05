import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import net.sf.json.JSONObject;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ServiceMinutePerformanceAnalyseBolt extends BaseRichBolt {

    private OutputCollector collector;
    private ConcurrentHashMap<String,ServiceRealtimePerformanceBean> analyseResultMinute = new ConcurrentHashMap<String, ServiceRealtimePerformanceBean>();

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        System.out.println("Service_analyse_minute_BOLT-prepare!");
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 1); //凌晨1点
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        Date date=calendar.getTime(); //第一次执行定时任务的时间
        //如果第一次执行定时任务的时间 小于当前的时间
        //此时要在 第一次执行定时任务的时间加一天，以便此任务在下个时间点执行。如果不加一天，任务会立即执行。
        //每分钟统计
        Timer minuteTimer = new Timer();
        minuteTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                Date now = new Date();
                System.out.println("to-analyse-mysql");
                for(String serviceName : analyseResultMinute.keySet()){
                    ServiceRealtimePerformanceBean analyse = analyseResultMinute.get(serviceName);
                    analyse.setStartTime(now.getTime());
                    collector.emit(new Values(serviceName,analyse.getInvokeTime(),analyse.getAverageTime(),analyse.getErrorTime(),analyse.getErrorPercentage(),analyse.getStartTime()));
                }
                analyseResultMinute.clear();
            }
        }, new Date(),60*1000);
    }

    public void execute(Tuple tuple) {
        //System.out.println("start-analyse");
        CoupleLogBean log = (CoupleLogBean)JSONObject.toBean(JSONObject.fromObject(tuple.getString(0)),CoupleLogBean.class);
        if ((log.getRequestCurrentMillis() != 0)&&(log.getResponseCurrentMillis()!=0)) {
            ServiceAnalyse serviceAnalyse = new ServiceAnalyse();
            ServiceRealtimePerformanceBean analyseBeanMinute;

            if (analyseResultMinute.get(log.getServiceName()) == null) {
                analyseBeanMinute = new ServiceRealtimePerformanceBean();
            } else {
                analyseBeanMinute = analyseResultMinute.get(log.getServiceName());
            }

            serviceAnalyse.analyseServicePerformance(analyseBeanMinute, log);

            analyseResultMinute.put(log.getServiceName(), analyseBeanMinute);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("serviceName","invokeTime","averageTime","errorTime","errorPercentage","startTime"));
    }
}
