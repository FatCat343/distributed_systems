import dao.NodeDao;
import dao.TagDao;
import dao.impl.NodeDaoImpl;
import dao.impl.TagDaoImpl;
import database.DBUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import service.NodeService;

import javax.xml.bind.JAXBException;
import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.sql.SQLException;

public class Main {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) throws IOException, XMLStreamException, JAXBException, SQLException {
        logger.info("start");
        DBUtil.init();
        NodeDao nodeDao = new NodeDaoImpl();
        TagDao tagDao = new TagDaoImpl();
        NodeService nodeService = new NodeService(nodeDao, tagDao);
        OsmProcessor osmProcessor = new OsmProcessor(nodeService);
        osmProcessor.processOsm();
        logger.info("finish");
    }
}
