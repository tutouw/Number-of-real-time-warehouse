import java.sql.*;

/**
 * @author Aaron
 * @date 2022/6/20 21:54
 */

public class Phoenix {
    // jdbc连接phoenix
    public static void main(String[] args) throws Exception {
        testSelect();
    }
    public static void testSelect() throws Exception {
        // Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        String url = "jdbc:phoenix:hadoop101,hadoop102,hadoop103";
        Connection conn = DriverManager.getConnection(url);
        Statement st = conn.createStatement();
        StringBuilder sql = new StringBuilder().append("create table if not exists GMALL_REALTIME.custom(id varchar primary key,names varchar)");
        System.out.println(sql);
        st.execute(String.valueOf(sql));
        /*while(rs.next()){
            int id = rs.getInt(1);
            String name = rs.getString(2);
            System.out.println(id + "/" +  name);
        }
        rs.close();*/
        st.close();
        conn.close();
    }
}
