package service;

import dao.NodeDao;
import dao.TagDao;
import lombok.AllArgsConstructor;
import entity.NodeEntity;
import entity.TagEntity;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@AllArgsConstructor
public class NodeService {
    private static final int BUFFER_SIZE = 500;
    private final NodeDao nodeDao;
    private final TagDao tagDao;

    private final List<NodeEntity> nodeBuffer = new ArrayList<>();

    public void putNode(NodeEntity node) {
        try {
            nodeDao.insertNode(node);
            for (TagEntity tag : node.getTags()) {
                tagDao.insertTag(tag);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void putNodeWithPreparedStatement(NodeEntity node) {
        try {
            nodeDao.insertPreparedNode(node);
            for (TagEntity tag : node.getTags()) {
                tagDao.insertPreparedTag(tag);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void putNodeBuffered(NodeEntity node) {
        nodeBuffer.add(node);
        if (nodeBuffer.size() == BUFFER_SIZE) {
            flush();
        }
    }

    public void flush() {
        try {
            nodeDao.batchInsertNodes(nodeBuffer);
            List<TagEntity> tags = nodeBuffer.stream()
                    .flatMap(node -> node.getTags().stream())
                    .collect(Collectors.toList());
            tagDao.batchInsertTags(tags);
            nodeBuffer.clear();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
