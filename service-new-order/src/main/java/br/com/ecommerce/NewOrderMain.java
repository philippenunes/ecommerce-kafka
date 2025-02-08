package br.com.ecommerce;

import br.com.ecommerce.dispatcher.KafkaDispatcher;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var orderDispatcher = new KafkaDispatcher<Order>()) {
            var email = Math.random() + "@email.com";
            for (int i = 0; i >= 0; i--) {

                var orderId = UUID.randomUUID().toString();
                var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);

                var order = new Order(orderId, amount, email);

                var id = new CorrelationId(NewOrderMain.class.getSimpleName());

                orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, id, order);
            }
        }
    }
}
