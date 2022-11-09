package gmbh.conteco;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Scanner;
import java.util.logging.Logger;

public class TransactionalProducer {

    public static void main(String[] args) throws IOException {
        Logger logger = Logger.getLogger(TransactionalProducer.class.getName());

        Properties properties = new Properties();

        try(InputStream stream = TransactionalProducer.class.
                getResourceAsStream("/application.properties")) {
            properties.load(stream);
        }

        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
            Scanner scanner = new Scanner(System.in);

            try {
                logger.info("Initializing transaction ...");
                producer.initTransactions();
                logger.info("Beginning transaction ...");
                producer.beginTransaction();
            }
            catch (RuntimeException e) {
                logger.fine("Error creating transaction: " + e.getMessage());
            }

            int messages_in_transaction = 0;
            long amount_of_message = 0;
            String topic = properties.getProperty("topic");

            while (true) {
                logger.info("Press enter to commit messages or type in a new message.");
                String text = scanner.nextLine();

                if (text.equals("")) {
                    if (messages_in_transaction == 0) {
                        logger.warning("Aborting producer ...");
                        producer.abortTransaction();
                        return;
                    }

                    producer.commitTransaction();
                    logger.info("Transaction committed.");
                    producer.beginTransaction();
                    messages_in_transaction = 0;
                    continue;
                }

                messages_in_transaction++;
                producer.send(new ProducerRecord<>(topic,
                        String.format("Messages in trans: %d, Number of msg: %d",
                                messages_in_transaction, amount_of_message), text));
                logger.info("Message has been sent.");
                amount_of_message++;
            }
        }
    }
}