package br.com.ecommerce;

import java.sql.*;

public class LocalDatabase {

    private final Connection connection;

    LocalDatabase(String name) throws SQLException {
        String url = "jdbc:sqlite:" + name + ".db";
        this.connection = DriverManager.getConnection(url);
    }

    public void createIfNotExists(String sql) {
        try {
            connection.createStatement().execute(sql);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public boolean update(String statement, String... params) throws SQLException {
        return prepare(statement, params).execute();
    }

    public ResultSet query(String statement, String... params) throws SQLException {
        return prepare(statement, params).executeQuery();
    }

    private PreparedStatement prepare(String statement, String[] params) throws SQLException {
        var preparedStatement = connection.prepareStatement(statement);
        for (int i = 0; i < params.length; i++) {
            preparedStatement.setString(i + 1, params[i]);
        }
        return preparedStatement;
    }

    public void close() throws SQLException {
        connection.close();
    }
}
