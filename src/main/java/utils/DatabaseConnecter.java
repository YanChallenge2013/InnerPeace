package utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

//Set up the connection for operations
public class DatabaseConnecter {
    //The user name and password for the connection
    private static String userName = "userName";
    private static String password = "password";
    //The host and port and database name
    private static String host = "127.0.0.1";
    private static String port = "3306";
    private static String databaseName = "databaseName";
    
    public static Connection getConnection() throws SQLException {
        Connection conn = null;
        Properties connectionProps = new Properties();
        connectionProps.put("user", userName);
        connectionProps.put("password", password);

        conn = DriverManager.getConnection(
                   "jdbc:" + "mysql" + "://" +
                   host + ":" + port+ "/" + databaseName,
                   connectionProps);
        System.out.println("Connected to database");
        return conn;
    }
}
