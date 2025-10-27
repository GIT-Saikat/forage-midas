package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListener {
    
    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);
    private final DatabaseConduit databaseConduit;

    public TransactionListener(DatabaseConduit databaseConduit) {
        this.databaseConduit = databaseConduit;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(Transaction transaction) {
        logger.info("Received transaction: {}", transaction);
        
        // Validate and process the transaction
        if (validateAndProcessTransaction(transaction)) {
            logger.info("Transaction processed successfully: {}", transaction);
        } else {
            logger.warn("Transaction discarded (validation failed): {}", transaction);
        }
    }

    private boolean validateAndProcessTransaction(Transaction transaction) {
        // Validate sender ID
        UserRecord sender = databaseConduit.findUserById(transaction.getSenderId());
        if (sender == null) {
            logger.warn("Invalid sender ID: {}", transaction.getSenderId());
            return false;
        }

        // Validate recipient ID
        UserRecord recipient = databaseConduit.findUserById(transaction.getRecipientId());
        if (recipient == null) {
            logger.warn("Invalid recipient ID: {}", transaction.getRecipientId());
            return false;
        }

        // Validate sender balance
        if (sender.getBalance() < transaction.getAmount()) {
            logger.warn("Insufficient balance for sender ID {}: has {}, needs {}", 
                       transaction.getSenderId(), sender.getBalance(), transaction.getAmount());
            return false;
        }

        // All validations passed - process the transaction
        try {
            // Update sender balance
            float newSenderBalance = sender.getBalance() - transaction.getAmount();
            databaseConduit.updateUserBalance(sender, newSenderBalance);

            // Update recipient balance
            float newRecipientBalance = recipient.getBalance() + transaction.getAmount();
            databaseConduit.updateUserBalance(recipient, newRecipientBalance);

            // Record the transaction
            TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount());
            databaseConduit.saveTransaction(transactionRecord);

            return true;
        } catch (Exception e) {
            logger.error("Error processing transaction: {}", transaction, e);
            return false;
        }
    }
}

