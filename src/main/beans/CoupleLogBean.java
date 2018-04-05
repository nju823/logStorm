import net.sf.json.JSONObject;
import net.sf.json.JSONString;

public class CoupleLogBean {

    private long traceId;
    private long spanId;
    private int type;      // 1,2,3
    private long parentSpanId;
    private String source;
    private String target;
    private String serviceUrl;
    private String serviceName;
    private String requestContent;
    private String responseContent;
    private long requestCurrentMillis;
    private long responseCurrentMillis;
    private long time;

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getTraceId() {
        return traceId;
    }

    public void setTraceId(long traceId) {
        this.traceId = traceId;
    }

    public long getSpanId() {
        return spanId;
    }

    public void setSpanId(long spanId) {
        this.spanId = spanId;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public long getParentSpanId() {
        return parentSpanId;
    }

    public void setParentSpanId(long parentSpanId) {
        this.parentSpanId = parentSpanId;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public String getServiceUrl() {
        return serviceUrl;
    }

    public void setServiceUrl(String serviceUrl) {
        this.serviceUrl = serviceUrl;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getRequestContent() {
        return requestContent;
    }

    public void setRequestContent(String requestContent) {
        this.requestContent = requestContent;
    }

    public String getResponseContent() {
        return responseContent;
    }

    public void setResponseContent(String responseContent) {
        this.responseContent = responseContent;
    }

    public long getRequestCurrentMillis() {
        return requestCurrentMillis;
    }

    public void setRequestCurrentMillis(long requestCurrentMillis) {
        this.requestCurrentMillis = requestCurrentMillis;
    }

    public long getResponseCurrentMillis() {
        return responseCurrentMillis;
    }

    public void setResponseCurrentMillis(long responseCurrentMillis) {
        this.responseCurrentMillis = responseCurrentMillis;
    }
}
