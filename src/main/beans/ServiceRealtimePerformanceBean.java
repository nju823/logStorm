

public class ServiceRealtimePerformanceBean {

    private String searviceName;
    private int invokeTime;
    private double averageTime;
    private int errorTime;
    private double errorPercentage;
    private Long startTime;

    public ServiceRealtimePerformanceBean(){
        this.invokeTime = 0;
        this.averageTime = 0;
        this.errorTime = 0;
        this.errorPercentage = 0;
    }

    public String getSearviceName() {
        return searviceName;
    }

    public void setSearviceName(String searviceName) {
        this.searviceName = searviceName;
    }

    public int getInvokeTime() {
        return invokeTime;
    }

    public void setInvokeTime(int invokeTime) {
        this.invokeTime = invokeTime;
    }

    public double getAverageTime() {
        return averageTime;
    }

    public void setAverageTime(double averageTime) {
        this.averageTime = averageTime;
    }

    public int getErrorTime() {
        return errorTime;
    }

    public void setErrorTime(int errorTime) {
        this.errorTime = errorTime;
    }

    public double getErrorPercentage() {
        return errorPercentage;
    }

    public void setErrorPercentage(double errorPercentage) {
        this.errorPercentage = errorPercentage;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }
}
