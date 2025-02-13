package br.com.ecommerce;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;

public class OrdersDatabase implements Closeable {

    private final LocalDatabase database;

    public OrdersDatabase() throws SQLException {
        this.database = new LocalDatabase("frauds_database");
        this.database.createIfNotExists("create table Orders (" +
                "uuid varchar(200) primary key," +
                "is_fraud boolean)");
    }

    public boolean saveNew(Order order) throws SQLException {
        if (wasProcessed(order)) {
            return false;
        }

        database.update("insert into Orders (uuid) values (?)", order.getOrderId());
        return true;
    }

    private boolean wasProcessed(Order order) throws SQLException {
        var results = database.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId());
        return results.next();
    }

    @Override
    public void close()  {
        try {
            database.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
