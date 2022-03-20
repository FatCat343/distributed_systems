package entity;

import generated.Node;
import lombok.Data;

import java.util.List;
import java.util.stream.Collectors;

@Data
public class NodeEntity {
    private long id;
    private String user;
    private Double longitude;
    private Double latitude;
    private List<TagEntity> tags;

    public NodeEntity(long id, String user, Double longitude, Double latitude, List<TagEntity> tags) {
        this.id = id;
        this.user = user;
        this.longitude = longitude;
        this.latitude = latitude;
        this.tags = tags;
    }

    public static NodeEntity convert(Node node) {
        List<TagEntity> tags = node.getTag().stream()
                .map(tag -> TagEntity.convert(tag, node.getId()))
                .collect(Collectors.toList());
        return new NodeEntity(node.getId(), node.getUser(), node.getLon(), node.getLat(), tags);
    }
}