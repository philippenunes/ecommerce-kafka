package br.com.ecommerce;

import br.com.ecommerce.dispatcher.KafkaDispatcher;
import br.com.ecommerce.consumer.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService {

    private final KafkaDispatcher<Order> orderKafkaDispatcher = new KafkaDispatcher<Order>();

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var fraudeService = new FraudDetectorService();
        try (var service = new KafkaService<>(FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudeService::parse,
                Map.of())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        System.out.println("----------------------------------------------------------------------------------------");
        System.out.println("processing new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value().getPayload());
        System.out.println(record.partition());
        System.out.println(record.offset());
        var message = record.value();

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        var order = message.getPayload();
        if (isFraud(order)) {
            System.out.println("order is fraud!" + order);
            orderKafkaDispatcher.send(
                    "ECOMMERCE_ORDER_REJECTED",
                    order.getEmail(),
                    message.getId().continueWith(FraudDetectorService.class.getSimpleName()),
                    order);
        } else {
            System.out.println("aproved: " + order);
            orderKafkaDispatcher.send(
                    "ECOMMERCE_ORDER_APROVED",
                    order.getEmail(),
                    message.getId().continueWith(FraudDetectorService.class.getSimpleName()),
                    order);
        }
    }

    private static boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
}
