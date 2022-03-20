package dao;

import entity.NodeEntity;

import java.sql.SQLException;
import java.util.List;

public interface NodeDao {

    void insertNode(NodeEntity node) throws SQLException;

    void insertPreparedNode(NodeEntity node) throws SQLException;

    void batchInsertNodes(List<NodeEntity> nodes) throws SQLException;

}
