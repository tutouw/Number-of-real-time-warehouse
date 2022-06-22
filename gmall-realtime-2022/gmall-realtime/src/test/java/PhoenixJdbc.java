import java.sql.*;
/**
 * @author Aaron
 * @date 2022/6/20 18:17
 */

public class PhoenixJdbc {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");

        //这里配置zookeeper的地址 可以是域名或者ip 可单个或者多个(用","分隔)
        String url = "jdbc:phoenix:hadoop101,hadoop102,hadoop103:2181";
        Connection conn = DriverManager.getConnection(url);
        Statement statement = conn.createStatement();

        ResultSet rs = statement.executeQuery("select * from GMALL_REALTIME.CUSTOMERS");
        while (rs.next()) {
            int pk = rs.getInt("PK");
            String col1 = rs.getString("COL1");

            System.out.println("PK=" + pk + ", COL1=" + col1);
        }
        // 关闭连接
        rs.close();

        statement.close();
        conn.close();
    }
}
