
import generated.Node;
import lombok.RequiredArgsConstructor;
import entity.NodeEntity;
import service.NodeService;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

@RequiredArgsConstructor
public class OsmProcessor {
    private final NodeService nodeService;

    public void processOsm() throws IOException, XMLStreamException, JAXBException {
        try (
            InputStream fileInputStream = Files.newInputStream(Paths.get("RU-NVS.osm"));
            StaxStreamHandler handler = new StaxStreamHandler(fileInputStream)
        ) {
            processDataFrom(handler);
        }
    }

    private void processDataFrom(StaxStreamHandler handler) throws XMLStreamException, JAXBException {
        XMLStreamReader reader = handler.getReader();
        JAXBContext jaxbContext = JAXBContext.newInstance(Node.class);
        Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();

        processNodeSection(reader, unmarshaller);
    }

    private void processNodeSection(XMLStreamReader reader, Unmarshaller unmarshaller) throws XMLStreamException, JAXBException {
        saveNodesWithExecuteQuery(reader, unmarshaller, 1_000);
        saveNodesWithPreparedStatement(reader, unmarshaller, 1_000);
        saveNodesBuffered(reader, unmarshaller, 1_000);
    }

    private void saveNodesWithPreparedStatement(XMLStreamReader reader, Unmarshaller unmarshaller, int nodesToProcess) throws XMLStreamException, JAXBException {
        int count = 0;
        Long time = 0L;
        while (reader.hasNext()) {
            int event = reader.next();
            if (isNodeSectionStart(reader, event)) {
                Node node = (Node) unmarshaller.unmarshal(reader);
                NodeEntity entity = NodeEntity.convert(node);

                long cur = System.currentTimeMillis();
                nodeService.putNodeWithPreparedStatement(entity);
                cur = System.currentTimeMillis() - cur;
                count++;

                time += cur;

                if (count % 1000 == 0 && count != 0) {
                    System.out.println("Strategy: PreparedStatement");
                    System.out.println("Current input objects: " + count);
                    System.out.println("Current time in ms for 1 object: " + time.doubleValue() / count);
                    System.out.println("Current time: " + time);
                    System.out.println("-----------------------------------");
                }

                if (count == nodesToProcess)
                    break;
            }
        }
    }

    private void saveNodesBuffered(XMLStreamReader reader, Unmarshaller unmarshaller, int nodesToProcess) throws XMLStreamException, JAXBException {
        int count = 0;
        Long time = 0L;
        while (reader.hasNext()) {
            int event = reader.next();
            if (isNodeSectionStart(reader, event)) {
                Node node = (Node) unmarshaller.unmarshal(reader);

                NodeEntity entity = NodeEntity.convert(node);

                long cur = System.currentTimeMillis();
                nodeService.putNodeBuffered(entity);
                cur = System.currentTimeMillis() - cur;
                count++;

                time += cur;

                if (count % 1000 == 0 && count != 0) {
                    System.out.println("Strategy: Buffered");
                    System.out.println("Current input objects: " + count);
                    System.out.println("Current time in ms for 1 object: " + time.doubleValue() / count);
                    System.out.println("Current time: " + time);
                    System.out.println("-----------------------------------");
                }

                if (count == nodesToProcess)
                    break;
            }
        }
    }

    private void saveNodesWithExecuteQuery(XMLStreamReader reader, Unmarshaller unmarshaller, int nodesToProcess) throws XMLStreamException, JAXBException {
        int count = 0;
        Long time = 0L;
        while (reader.hasNext()) {
            int event = reader.next();
            if (isNodeSectionStart(reader, event)) {
                Node node = (Node) unmarshaller.unmarshal(reader);

                NodeEntity entity = NodeEntity.convert(node);

                long cur = System.currentTimeMillis();
                nodeService.putNode(entity);
                cur = System.currentTimeMillis() - cur;
                count++;

                time += cur;

                if (count % 1000 == 0 && count != 0) {
                    System.out.println("Strategy: ExecuteQuery");
                    System.out.println("Current input objects: " + count);
                    System.out.println("Current time in ms for 1 object: " + time.doubleValue() / count);
                    System.out.println("Current time: " + time);
                    System.out.println("-----------------------------------");
                }

                if (count == nodesToProcess)
                    break;
            }
        }
    }

    private boolean isNodeSectionStart(XMLStreamReader reader, int event) {
        return event == XMLEvent.START_ELEMENT && "node".equals(reader.getLocalName());
    }
}


