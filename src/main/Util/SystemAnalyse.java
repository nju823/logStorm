import java.text.DecimalFormat;

public class SystemAnalyse {
    public void anslyseSystemPerformance(SystemRealtimePerformanceBean systemRealtimePerformanceBean,CoupleLogBean log){
        DecimalFormat df = new DecimalFormat("0.0000");

        systemRealtimePerformanceBean.setInvokeTime(systemRealtimePerformanceBean.getInvokeTime() + 1);

        if (log.getType() == 3) {
            systemRealtimePerformanceBean.setErrorTime(systemRealtimePerformanceBean.getErrorTime() + 1);
            systemRealtimePerformanceBean.setErrorPercentage(Double.parseDouble(df.format(systemRealtimePerformanceBean.getErrorTime() / (float) systemRealtimePerformanceBean.getInvokeTime())));
        }
    }
}
