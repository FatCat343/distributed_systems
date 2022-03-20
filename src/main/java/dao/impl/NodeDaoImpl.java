package dao.impl;

import dao.NodeDao;
import database.DBUtil;
import entity.NodeEntity;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class NodeDaoImpl implements NodeDao {
    private static final String SQL_INSERT = "insert into nodes(id, username, longitude, latitude) " + "values (?, ?, ?, ?)";

    @Override
    public void insertNode(NodeEntity node) throws SQLException {
        Connection connection = DBUtil.getConnection();
        Statement statement = connection.createStatement();
        String sql = "insert into nodes(id, username, longitude, latitude) " +
                "values (" + node.getId() + ", '" + node.getUser().replaceAll("'", "''") + "', " + node.getLongitude() +
                ", " + node.getLatitude() + ")";
        statement.execute(sql);
    }

    @Override
    public void insertPreparedNode(NodeEntity node) throws SQLException {
        Connection connection = DBUtil.getConnection();
        PreparedStatement statement = connection.prepareStatement(SQL_INSERT);
        prepareStatement(statement, node);
        statement.execute();
    }

    @Override
    public void batchInsertNodes(List<NodeEntity> nodes) throws SQLException {
        Connection connection = DBUtil.getConnection();
        PreparedStatement statement = connection.prepareStatement(SQL_INSERT);
        for (NodeEntity node : nodes) {
            prepareStatement(statement, node);
            statement.addBatch();
        }
        statement.executeBatch();
    }

    private static void prepareStatement(PreparedStatement statement, NodeEntity node) throws SQLException {
        statement.setLong(1, node.getId());
        statement.setString(2, node.getUser());
        statement.setDouble(3, node.getLongitude());
        statement.setDouble(4, node.getLatitude());
    }
}
