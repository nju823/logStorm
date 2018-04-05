import java.text.DecimalFormat;

public class ServiceAnalyse {
    public void analyseServicePerformance(ServiceRealtimePerformanceBean analyse,CoupleLogBean log) {
            DecimalFormat df = new DecimalFormat("0.0000");

            analyse.setInvokeTime(analyse.getInvokeTime() + 1);

            analyse.setSystem(log.getTarget());

            analyse.setMaxTime(Math.max(analyse.getMaxTime(),(int)log.getTime()));

            analyse.setAverageTime(Double.parseDouble(df.format((analyse.getAverageTime() * (analyse.getInvokeTime() - 1) + log.getTime()) / ((float) analyse.getInvokeTime()))));

            if (log.getType() == 3) {
                analyse.setErrorTime(analyse.getErrorTime() + 1);
                analyse.setErrorPercentage(Double.parseDouble(df.format(analyse.getErrorTime() / (float) analyse.getInvokeTime())));
            }
        }
}
