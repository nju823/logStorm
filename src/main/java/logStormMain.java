import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy.TimeUnit;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import storm.kafka.*;

import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class logStormMain {
    public static void main(String[] args) throws InterruptedException {
        //kfk信息
        String zks = "120.79.201.3:4181,39.107.107.237:4181,120.79.201.107:4181";
        String topic = "access_log_topic";
        String zkRoot = ""; // default zookeeper root configuration for storm
        String id = "access_log_topic";

        //kfk Spout
        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.ignoreZkOffsets = true;
        spoutConf.zkServers = Arrays.asList(new String[] {"120.79.201.3","39.107.107.237","120.79.201.107"});
        spoutConf.zkPort = 4181;

        //HDFS bolt
        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("\t"); // use "\t" instead of "," for field delimiter
        SyncPolicy syncPolicy = new CountSyncPolicy(10000); // sync the filesystem after every 1k tuples*10000
        FileRotationPolicy rotationPolicy = new TimedRotationPolicy(1.0f, TimeUnit.DAYS); // rotate files
        Calendar now = Calendar.getInstance();
        String date =  now.get(Calendar.YEAR) + "" + (now.get(Calendar.MONTH) + 1) + "" + now.get(Calendar.DAY_OF_MONTH);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/log_"+date+"/")
                .withPrefix("LogSystem").withExtension(".log");// set file name format
        HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl("hdfs://106.15.177.156:8020")
                .withFileNameFormat(fileNameFormat).withRecordFormat(format)
                .withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy);

        //ES bolt
        Map<Object, Object> boltConf = new HashMap<Object, Object>();
        boltConf.put("es.nodes", "47.100.34.12:9200");
        boltConf.put("es.index.auto.create", "true");
        boltConf.put("es.ser.writer.bytes.class", "org.platform.storm.elasticsearch.bolt.StormTupleBytesConverter");

        //jdbc bolt
        MySQLConnectionProvide connection = new MySQLConnectionProvide();
        JdbcMapper mapper_service_minute = new SimpleJdbcMapper("realtime_service_analyse_minute",connection);
        JdbcInsertBolt jdbcBolt_service_minute = new JdbcInsertBolt(connection,mapper_service_minute)
                .withTableName("realtime_service_analyse_minute")
                .withQueryTimeoutSecs(30);

        JdbcMapper mapper_service_day = new SimpleJdbcMapper("realtime_service_analyse_day",connection);
        JdbcInsertBolt jdbcBolt_service_day = new JdbcInsertBolt(connection,mapper_service_day)
                .withTableName("realtime_service_analyse_day")
                .withQueryTimeoutSecs(30);

        //定义拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("log-reader", new KafkaSpout(spoutConf),5);
        builder.setBolt("log-check", new logCheckBolt(),2).shuffleGrouping("log-reader");
        builder.setBolt("log-to-HDFS",new logNormalizerToHDFSBolt(),2).shuffleGrouping("log-check");
        builder.setBolt("HDFS",hdfsBolt,2).fieldsGrouping("log-to-HDFS",new Fields("HDFS-log"));
        builder.setBolt("log-to-ES",new logNormalizerToESBolt(),2).shuffleGrouping("log-check");
        //builder.setBolt("ES",new EsBolt("data/telecom",boltConf)).fieldsGrouping("log-to-ES",new Fields("ES-log"));

        builder.setBolt("service-analyse-minute",new ServiceMinutePerformanceAnalyseBolt(),1).fieldsGrouping("log-to-ES",new Fields("couple-log"));
        builder.setBolt("save-service-analyse-minute",jdbcBolt_service_minute,2).shuffleGrouping("service-analyse-minute");

        builder.setBolt("service-analyse-day",new ServiceDayPerformanceAnalyseBolt(),1).fieldsGrouping("log-to-ES",new Fields("couple-log"));
        builder.setBolt("save-service-analyse-day",jdbcBolt_service_day,2).shuffleGrouping("service-analyse-day");

        //配置
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(1);

        //运行拓扑
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting-Started-Topologie", conf, builder.createTopology());
//        Thread.sleep(10000);
//        cluster.shutdown();
    }
}
