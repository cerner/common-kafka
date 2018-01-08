package com.cerner.common.kafka;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Wrapper exception to indicate an error occurred while trying to submit to the destination
 * brokers. The exception may or not be recoverable. To see the list of possible exceptions encompassed can be found at
 * {@link org.apache.kafka.clients.producer.Callback#onCompletion(org.apache.kafka.clients.producer.RecordMetadata, Exception)}.
 *
 * If the exception is recoverable, retrying the message might fix the issue.
 *
 * @author Stephen Durfey
 */
public class KafkaExecutionException extends IOException {

    /**
     * Serial version UID
     */
    private static final long serialVersionUID = -6449179138732948352L;

    public KafkaExecutionException(ExecutionException e) {
        super(e);
    }
}