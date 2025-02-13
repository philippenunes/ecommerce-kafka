package br.com.ecommerce;

import br.com.ecommerce.consumer.ConsumerService;
import br.com.ecommerce.consumer.ServiceRunner;
import br.com.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService implements ConsumerService<Order> {

    private final LocalDatabase database;

    public FraudDetectorService() throws SQLException {
        this.database = new LocalDatabase("frauds_database");
        this.database.createIfNotExists("create table Orders (" +
                "uuid varchar(200) primary key," +
                "is_fraud boolean)");
    }

    private final KafkaDispatcher<Order> orderKafkaDispatcher = new KafkaDispatcher<Order>();

    public static void main(String[] args)  {
       new ServiceRunner<>(FraudDetectorService::new).start(1);
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException, SQLException {
        System.out.println("----------------------------------------------------------------------------------------");
        System.out.println("processing new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value().getPayload());
        System.out.println(record.partition());
        System.out.println(record.offset());
        var message = record.value();
        var order = message.getPayload();

        if (wasProcessed(order)) {
            System.out.println("order " + order.getOrderId() + " was already processed");
            return;
        }

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (isFraud(order)) {
            database.update("insert into Orders (uuid, is_fraud) values (?, true)", order.getOrderId());
            System.out.println("order is a fraud! " + order);
            orderKafkaDispatcher.send(
                    "ECOMMERCE_ORDER_REJECTED",
                    order.getEmail(),
                    message.getId().continueWith(FraudDetectorService.class.getSimpleName()),
                    order);
        } else {
            database.update("insert into Orders (uuid, is_fraud) values (?, false)", order.getOrderId());
            System.out.println("aproved: " + order);
            orderKafkaDispatcher.send(
                    "ECOMMERCE_ORDER_APROVED",
                    order.getEmail(),
                    message.getId().continueWith(FraudDetectorService.class.getSimpleName()),
                    order);
        }
    }

    private boolean wasProcessed(Order order) throws SQLException {
        var results = database.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId());
        return results.next();
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return FraudDetectorService.class.getSimpleName();
    }

    private static boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
}
