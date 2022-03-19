import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class OsmProcessor {
    private final Map<String, Integer> tagNodesCount = new HashMap<>();
    private final Map<String, Integer> userNodesCount = new HashMap<>();

    public void processOsm() throws IOException, XMLStreamException {
        try (
            var fileInputStream = Files.newInputStream(Paths.get("RU-NVS.osm"));
            var handler = new StaxStreamHandler(fileInputStream)
        ) {
            accumulateDataFrom(handler);

            printSortedChangesCountPerUser();
            printMap(tagNodesCount, "#2 Count of TAG [%s] in nodes: %s%n");
        }
    }

    private void accumulateDataFrom(StaxStreamHandler handler) throws XMLStreamException {
        var reader = handler.getReader();
        CurrentUser currentUser = null;
        var openedNodeSection = false;
        while (reader.hasNext()) {
            var event = reader.next();
            if (isNodeSectionStart(reader, event)) {
                openedNodeSection = true;
                currentUser = processNodeSection(reader, currentUser);
            }
            if (isTagSection(reader, openedNodeSection, event)){
                processTagSection(reader);
            }
            if (isNodeSectionFinish(reader, event)){
                openedNodeSection = false;
            }
        }
    }

    private void printSortedChangesCountPerUser() {
        userNodesCount.entrySet().stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .map(it -> "#1 Username: " + it.getKey() + " count of user changes: " + it.getValue())
                .forEach(System.out::println);
    }

    private void printMap(Map<String, Integer> sortedTagsMap, String pattern) {
        for (Map.Entry<String, Integer> entry : sortedTagsMap.entrySet())
            System.out.printf(pattern, entry.getKey(), entry.getValue());
    }

    private void processTagSection(XMLStreamReader reader) {
        var tagKey = extractKeyFromTagSection(reader);
        saveChangesToMap(tagNodesCount, tagKey, 1);
    }

    private CurrentUser processNodeSection(XMLStreamReader reader, CurrentUser currentUser) {
        if (currentUser == null) {
            currentUser = new CurrentUser(extractUsernameFromNodeSection(reader));
        } else if (!currentUser.getName().equals(extractUsernameFromNodeSection(reader))) {
            saveChangesToMap(userNodesCount, currentUser.getName(), currentUser.getNodes());
            currentUser = new CurrentUser(extractUsernameFromNodeSection(reader));
        }
        currentUser.increaseNodesByOne();
        return currentUser;
    }

    private void saveChangesToMap(Map<String, Integer> map, String key, int valueDelta) {
        map.merge(key, valueDelta, Integer::sum);
    }

    private String extractUsernameFromNodeSection(XMLStreamReader reader) {
        return reader.getAttributeValue(4);
    }

    private String extractKeyFromTagSection(XMLStreamReader reader) {
        return reader.getAttributeValue(0);
    }

    private boolean isNodeSectionFinish(XMLStreamReader reader, int event) {
        return event == XMLEvent.END_ELEMENT && "node".equals(reader.getLocalName());
    }

    private boolean isNodeSectionStart(XMLStreamReader reader, int event) {
        return event == XMLEvent.START_ELEMENT && "node".equals(reader.getLocalName());
    }

    private boolean isTagSection(XMLStreamReader reader, boolean opened, int event) {
        return event == XMLEvent.START_ELEMENT && "tag".equals(reader.getLocalName()) && opened;
    }

    private static class CurrentUser {
        private final String name;
        private int nodes;

        public CurrentUser(String name) {
            this.name = name;
            this.nodes = 0;
        }

        public String getName() {
            return name;
        }

        public int getNodes() {
            return nodes;
        }

        public void increaseNodesByOne() {
            nodes++;
        }
    }
}


