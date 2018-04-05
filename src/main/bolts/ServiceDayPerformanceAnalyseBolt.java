import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import net.sf.json.JSONObject;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

public class ServiceDayPerformanceAnalyseBolt extends BaseRichBolt {

    private OutputCollector collector;
    private Map<String,ServiceRealtimePerformanceBean> analyseResultDay = new HashMap<String, ServiceRealtimePerformanceBean>();

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        System.out.println("Service_analyse_day_BOLT-prepare!");
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 1); //凌晨1点
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        Date date=calendar.getTime(); //第一次执行定时任务的时间
        //如果第一次执行定时任务的时间 小于当前的时间
        //此时要在 第一次执行定时任务的时间加一天，以便此任务在下个时间点执行。如果不加一天，任务会立即执行。
        //每天统计
        Timer dayTimer = new Timer();
        dayTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                Date now = new Date();
                Iterator<Map.Entry<String,ServiceRealtimePerformanceBean>> iterator = analyseResultDay.entrySet().iterator();
                System.out.println("to-service-day-mysql!");
                while (iterator.hasNext()){
                    Map.Entry<String,ServiceRealtimePerformanceBean> entry = iterator.next();
                    ServiceRealtimePerformanceBean analyse = entry.getValue();
                    analyse.setStartTime(now.getTime());
                    collector.emit(new Values(entry.getKey(),analyse.getInvokeTime(),analyse.getAverageTime(),analyse.getErrorTime(),analyse.getErrorPercentage(),analyse.getStartTime()));
                }
            }
        },date,60*1000);

        dayTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                analyseResultDay.clear();
            }
        },date,26*60*60*1000);
    }

    public void execute(Tuple tuple) {
        CoupleLogBean log = (CoupleLogBean) JSONObject.toBean(JSONObject.fromObject(tuple.getString(0)),CoupleLogBean.class);
        if ((log.getRequestCurrentMillis() != 0)&&(log.getResponseCurrentMillis()!=0)) {
            ServiceAnalyse serviceAnalyse = new ServiceAnalyse();
            ServiceRealtimePerformanceBean analyseBeanDay;

            if (analyseResultDay.get(log.getServiceName()) == null) {
                analyseBeanDay = new ServiceRealtimePerformanceBean();
            } else {
                analyseBeanDay = analyseResultDay.get(log.getServiceName());
            }

            serviceAnalyse.analyseServicePerformance(analyseBeanDay, log);

            analyseResultDay.put(log.getServiceName(), analyseBeanDay);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("serviceName","invokeTime","averageTime","errorTime","errorPercentage","insertTime"));
    }
}
