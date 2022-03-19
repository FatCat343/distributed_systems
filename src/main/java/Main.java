import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;

public class Main {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) throws IOException, XMLStreamException {
        logger.info("start");
        var osmProcessor = new OsmProcessor();
        osmProcessor.processOsm();
        logger.info("finish");
    }
}
