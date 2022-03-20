package dao;

import entity.TagEntity;

import java.sql.SQLException;
import java.util.List;

public interface TagDao {

    void insertTag(TagEntity tag) throws SQLException;

    void insertPreparedTag(TagEntity tag) throws SQLException;

    void batchInsertTags(List<TagEntity> tags) throws SQLException;
}
