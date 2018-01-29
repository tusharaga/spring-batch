package listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.listener.ItemListenerSupport;

public class ItemFailureLoggerListener extends ItemListenerSupport {

    private static final Logger logger = LoggerFactory.getLogger(ItemFailureLoggerListener.class);

    public void onReadError(Exception ex) {
        logger.error("Encountered error on read", ex);
    }

    public void onWriteError(Exception ex, Object item) {
        logger.error("Encountered error on write", ex);
    }

}