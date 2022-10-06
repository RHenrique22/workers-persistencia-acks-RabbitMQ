package br.edu.ifpb.prog_dist;

import java.nio.charset.StandardCharsets;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class Consumidor {

    private static void doWork(String task) throws InterruptedException {
        for(char ch: task.toCharArray()) {
            if(ch == '.') {
                try {
                    Thread.sleep(1000);
                } catch(InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {

        final String NAME_QUEUE = "PROGDIST_2022";
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("mqadmin");
        factory.setPassword("Admin123XX_");

        System.out.println("Aguardando mensagens...\nPara sair, pressione CTRL + C");

        Connection connection = factory.newConnection();
        Channel    channel    = connection.createChannel();

        channel.basicQos(1);

        channel.queueDeclare(NAME_QUEUE, true, false, false, null);

        DeliverCallback callback = (consumerTag, deliver) -> {
            String mensagem = new String(deliver.getBody(), StandardCharsets.UTF_8);
            System.out.printf("A mensagem recebida foi '%s'%n", mensagem);

            try {
                doWork(mensagem);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                System.out.println("Finalizado");
                channel.basicAck(deliver.getEnvelope().getDeliveryTag(), false);
            }
        };

        channel.basicConsume(NAME_QUEUE, false, callback, consumerTag -> {});

    }
}
