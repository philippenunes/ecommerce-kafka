package br.com.ecommerce;

import br.com.ecommerce.dispatcher.KafkaDispatcher;
import br.com.ecommerce.consumer.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class BatchSendMessageService {

    private final KafkaDispatcher<User> userDispatcher = new KafkaDispatcher<>();
    private final Connection connection;

    BatchSendMessageService() throws SQLException {
        String url = "jdbc:sqlite:users_database.db";
        this.connection = DriverManager.getConnection(url);
        try {
            connection.createStatement().execute("create table Users (" +
                    "uuid varchar(200) primary key," +
                    "email varchar(200))");
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }


    public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {
        var batchService = new BatchSendMessageService();
        try(var service = new KafkaService<>(BatchSendMessageService.class.getSimpleName(),
                "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS",
                batchService::parse,
                Map.of())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<String>> record) throws SQLException, ExecutionException, InterruptedException {
        System.out.println("----------------------------------------------------------------------------------------");
        System.out.println("processing new batch, checking for new user");
        System.out.println("topic: " + record.value().getPayload());
        var message = record.value();

        if (true) throw new RuntimeException("forced error! ;)");

        for (User user : getAllUsers()) {
            userDispatcher.sendAsync(
                    message.getPayload(),
                    user.getUuid(),
                    message.getId().continueWith(BatchSendMessageService.class.getSimpleName()),
                    user);
            System.out.println("send to " + user);
        }

    }

    private List<User> getAllUsers() throws SQLException {
        var results = connection.prepareStatement("select uuid from Users").executeQuery();
        List<User> users = new ArrayList<>();
        while (results.next()) {
            users.add(new User(results.getString(1)));
        }
        return users;
    }
}
