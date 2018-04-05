import backtype.storm.tuple.Values;
import com.alibaba.fastjson.JSONObject;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class test {
    private  ConcurrentHashMap<String,String> map = new ConcurrentHashMap<String, String>();
    test(){
        final Timer minuteTimer = new Timer();
        minuteTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                System.out.println(map.size());
            }
        }, new Date(),5*1000);
    }


    public void add(){
        map.put(new Date().toString(),"A");
    }


    public static void main(String[] args) {
        final test t = new test();
        Timer minuteTimer2 = new Timer();
        minuteTimer2.schedule(new TimerTask() {
            @Override
            public void run() {
                t.add();
                System.out.println(t.map.size()+"timer2");
            }
        }, new Date(),1000);
    }
}
