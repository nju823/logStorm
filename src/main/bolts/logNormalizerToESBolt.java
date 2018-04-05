import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import net.sf.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;

public class logNormalizerToESBolt  extends BaseRichBolt {

    private OutputCollector collector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        System.out.println("to-ES_BOLT-prepare!");
    }

    public void execute(Tuple tuple) {
        SingleLogBean singleLog =com.alibaba.fastjson.JSONObject.parseObject(tuple.getString(0), SingleLogBean.class);
        CoupleLogBean coupleLog;

        String sameSpanIdLogStr=sendPost("http://47.100.34.12:1346/searchdata/", "index=log_index&type=log_table&searchbody={\"query\":{\"match\":{\"spanId\": \""+singleLog.getSpanId()+"\"}}}");
        //System.out.println("search:"+sameSpanIdLogStr);
        JSONObject sameSpanIdLog = JSONObject.fromObject(sameSpanIdLogStr);
        int existSameId =0;
        if(!sameSpanIdLog.getJSONObject("hits").isNullObject())
            existSameId = sameSpanIdLog.getJSONObject("hits").getInt("total");

        if(existSameId == 0){
            coupleLog = new CoupleLogBean();
        }else{
            coupleLog = com.alibaba.fastjson.JSONObject.parseObject(sameSpanIdLog.getJSONObject("hits").getJSONArray("hits").getJSONObject(0).getJSONObject("_source").toString(),CoupleLogBean.class);
        }

        if(singleLog.getType()==1 || singleLog.getType()==3){
            coupleLog.setParentSpanId(singleLog.getParentSpanId());
            coupleLog.setRequestContent(singleLog.getContent());
            coupleLog.setRequestCurrentMillis(singleLog.getCurrentMillis());
            coupleLog.setServiceName(singleLog.getServiceName());
            coupleLog.setServiceUrl(singleLog.getServiceUrl());
            coupleLog.setSource(singleLog.getSource());
            coupleLog.setTarget(singleLog.getTarget());
            coupleLog.setSpanId(singleLog.getSpanId());
            coupleLog.setTraceId(singleLog.getTraceId());
            coupleLog.setTime(coupleLog.getResponseCurrentMillis()-coupleLog.getRequestCurrentMillis());
            if(coupleLog.getType() != 3){
                if(singleLog.getType() == 1 ){
                    coupleLog.setType(1);
                }else if(singleLog.getType() == 3 ){
                    coupleLog.setType(2);
                }
            }

        }else{
            coupleLog.setSpanId(singleLog.getSpanId());
            coupleLog.setResponseContent(singleLog.getContent());
            coupleLog.setResponseCurrentMillis(singleLog.getCurrentMillis());
            coupleLog.setTime(coupleLog.getResponseCurrentMillis()-coupleLog.getRequestCurrentMillis());
            if(singleLog.getType() == 2 ){
                coupleLog.setType(1);
            }else if(singleLog.getType() == 4){
                coupleLog.setType(2);
            }else{
                coupleLog.setType(3);
            }
        }

        String result;
        if(existSameId == 0){
            result =sendPost("http://47.100.34.12:1346/syncdata/", "index=log_index&type=log_table&syncbody="+JSONObject.fromObject(coupleLog).toString());
        }else{
            String documentId = sameSpanIdLog.getJSONObject("hits").getJSONArray("hits").getJSONObject(0).getString("_id");
            result =sendPost("http://47.100.34.12:1346/syncdata/", "index=log_index&type=log_table&docid="+documentId+"&syncbody="+JSONObject.fromObject(coupleLog).toString());
            collector.emit(tuple, new Values(JSONObject.fromObject(coupleLog).toString()));
            //System.out.println("es-to-analyse!");
        }
        //System.out.println("to-ES:"+result);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("couple-log"));
    }

    public static String sendPost(String url, String param) {
        PrintWriter out = null;
        BufferedReader in = null;
        String result = "";
        try {
            URL realUrl = new URL(url);
            // 打开和URL之间的连接
            URLConnection conn = realUrl.openConnection();
            // 设置通用的请求属性
            conn.setRequestProperty("accept", "*/*");
            conn.setRequestProperty("connection", "Keep-Alive");
            conn.setRequestProperty("user-agent",
                    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            // 发送POST请求必须设置如下两行
            conn.setDoOutput(true);
            conn.setDoInput(true);
            // 获取URLConnection对象对应的输出流
            out = new PrintWriter(conn.getOutputStream());
            // 发送请求参数
            out.print(param);
            // flush输出流的缓冲
            out.flush();
            // 定义BufferedReader输入流来读取URL的响应
            in = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                result += line;
            }
        } catch (Exception e) {
            System.out.println("发送 POST 请求出现异常！"+e);
            e.printStackTrace();
        }
        //使用finally块来关闭输出流、输入流
        finally{
            try{
                if(out!=null){
                    out.close();
                }
                if(in!=null){
                    in.close();
                }
            }
            catch(IOException ex){
                ex.printStackTrace();
            }
        }
        return result;
    }

}
