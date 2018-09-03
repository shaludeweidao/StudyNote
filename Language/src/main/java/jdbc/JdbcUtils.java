package jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public final class JdbcUtils {

    private static String url = "jdbc:mysql://localhost:3306/spark";
    private static String user = "root";
    private static String password = "root";



    private JdbcUtils(){}


    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }



    public static void freeSource(Statement st, Connection conn){
        try {
            if (st != null ){ st.close(); }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    System.out.println("close conn is failed");
                    e.printStackTrace();
                }
            }
        }
    }




}
