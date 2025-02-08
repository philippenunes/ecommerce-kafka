package br.com.ecommerce;

import br.com.ecommerce.consumer.ConsumerService;
import br.com.ecommerce.consumer.ServiceRunner;
import br.com.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.ExecutionException;

public class EmailNewOrderService implements ConsumerService<Order> {

    private final KafkaDispatcher<String> emailDispactcher = new KafkaDispatcher<>();

    public static void main(String[] args)  {
        new ServiceRunner<>(EmailNewOrderService::new).start(1);
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        System.out.println("----------------------------------------------------------------------------------------");
        System.out.println("processing new order, preparing email");
        System.out.println(record.value());

        var emailCode = "we are processing your order!";
        var order = record.value().getPayload();
        var id = record.value().getId().continueWith(EmailNewOrderService.class.getSimpleName());
        emailDispactcher.send("ECOMMERCE_SEND_EMAIL",
                order.getEmail(),
                id,
                emailCode);
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return EmailNewOrderService.class.getSimpleName();
    }
}
