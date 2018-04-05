import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import net.sf.json.JSONObject;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SystemDayPerformanceAnalyseBolt extends BaseRichBolt {

    private OutputCollector collector;
    private ConcurrentHashMap<String,SystemRealtimePerformanceBean> systemPerformanceAnalyseResult = new ConcurrentHashMap<String, SystemRealtimePerformanceBean>();

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        System.out.println("System_analyse_day_BOLT-prepare!");
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 1); //凌晨1点
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        Date date=calendar.getTime(); //第一次执行定时任务的时间
        //如果第一次执行定时任务的时间 小于当前的时间
        //此时要在 第一次执行定时任务的时间加一天，以便此任务在下个时间点执行。如果不加一天，任务会立即执行。
        //每分钟统计
        Timer dayTimer = new Timer();
        dayTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                Date now = new Date();
                System.out.println("to-analyse-mysql-system-day");
                for(String systemName : systemPerformanceAnalyseResult.keySet()){
                    SystemRealtimePerformanceBean analyse = systemPerformanceAnalyseResult.get(systemName);
                    analyse.setInsertTime(now.getTime());
                    collector.emit(new Values(systemName,analyse.getInvokeTime(),analyse.getErrorTime(),analyse.getErrorPercentage(),analyse.getInsertTime()));
                }
            }
        }, new Date(),60*1000);
        dayTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                systemPerformanceAnalyseResult.clear();
            }
        },date,26*60*60*1000);
    }

    public void execute(Tuple tuple) {
        CoupleLogBean log = (CoupleLogBean) JSONObject.toBean(JSONObject.fromObject(tuple.getString(0)),CoupleLogBean.class);
        if ((log.getRequestCurrentMillis() != 0)&&(log.getResponseCurrentMillis()!=0)) {
            SystemAnalyse systemAnalyse = new SystemAnalyse();
            SystemRealtimePerformanceBean systemRealtimePerformanceBean;

            if(systemPerformanceAnalyseResult.get(log.getTarget()) == null){
                systemRealtimePerformanceBean = new SystemRealtimePerformanceBean();
            }else{
                systemRealtimePerformanceBean = systemPerformanceAnalyseResult.get(log.getTarget());
            }

            systemAnalyse.anslyseSystemPerformance(systemRealtimePerformanceBean,log);

            systemPerformanceAnalyseResult.put(log.getTarget(),systemRealtimePerformanceBean);

        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("systemName","invokeTime","errorTime","errorPercentage","insertTime"));

    }
}
