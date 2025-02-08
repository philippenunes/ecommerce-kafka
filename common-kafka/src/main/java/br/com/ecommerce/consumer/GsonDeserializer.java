package br.com.ecommerce.consumer;

import br.com.ecommerce.Message;
import br.com.ecommerce.MessageAdapder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;

public class GsonDeserializer implements Deserializer<Message> {

    private final Gson gson = new GsonBuilder().registerTypeAdapter(Message.class, new MessageAdapder()).create();

    @Override
    public Message deserialize(String s, byte[] bytes) {
        return gson.fromJson(new String(bytes), Message.class);
    }
}
