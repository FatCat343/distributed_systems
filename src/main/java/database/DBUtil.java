package database;

import org.flywaydb.core.Flyway;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBUtil {
    public static final String DATABASE_URL = "jdbc:postgresql://localhost:5432/test_db";
    public static final String DATABASE_USERNAME = "postgres";
    public static final String DATABASE_PASSWORD = "12345";

    private static Connection connection;

    public static void init() throws SQLException {
        Flyway flyway = Flyway.configure().dataSource(DATABASE_URL, DATABASE_USERNAME, DATABASE_PASSWORD).load();
        flyway.migrate();
        connection = DriverManager.getConnection(DATABASE_URL, DATABASE_USERNAME, DATABASE_PASSWORD);
    }

    public static Connection getConnection() {
        return connection;
    }

    public static void closeConnection() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }
}
