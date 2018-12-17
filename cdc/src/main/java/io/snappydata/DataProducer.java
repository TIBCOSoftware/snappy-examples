package io.snappydata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

public class DataProducer {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        System.out.println("Getting connection");
        Connection conn = null;
        try {
            String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
            Class.forName(driver);
            String url;
            url = "jdbc:sqlserver://sqlent.westus.cloudapp.azure.com:1433";
            String username = "sqldb";
            String password = "snappydata#msft1";
            Properties props = new Properties();
            props.put("username", username);
            props.put("password", password);
            // Connection connection ;
            conn = DriverManager.getConnection(url, props);
            String query = "INSERT INTO testdatabase.dbo.ADJUSTMENT VALUES (?, ?, 9929, 1, 506161, 775324, 431703, 28440200.7189, N'iT', N'2016-05-09', 520989, 966723, 592283, 682854, 165363, N'2016-07-02', N'2016-05-30', N'889', N'89')";
            PreparedStatement preparedStatement = conn.prepareStatement(query);
conn.setAutoCommit(false);
            for(int i=1 ;i< 10000000; i++){
                preparedStatement.setInt(1,i);
                preparedStatement.setInt(2,i*2);
                preparedStatement.addBatch();
                if(i%3000 == 0){
                    preparedStatement.executeBatch();
                    conn.commit();
                }
            }

            preparedStatement.executeBatch();
            preparedStatement.close();
            conn.commit();
        } finally {
            if(conn!=null) {
                conn.close();
            }
        }
    }
}
