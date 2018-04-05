import org.apache.storm.jdbc.common.ConnectionProvider;
import java.sql.Connection;
import java.sql.DriverManager;


public class MySQLConnectionProvide implements ConnectionProvider {

    private static String driver = "com.mysql.jdbc.Driver";
    private static String url = "jdbc:mysql://rm-wz991a6b04vm5v4q4zo.mysql.rds.aliyuncs.com:3306/pepple";
    private static String user = "root";
    private static String password = "Pan12345";

    public void prepare() {

    }

    public Connection getConnection() {
        try {
            Class.forName(driver);
            return DriverManager.getConnection(url,user,password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void cleanup() {

    }
}
