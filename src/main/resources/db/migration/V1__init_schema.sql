DROP TABLE IF EXISTS tags;
DROP TABLE IF EXISTS nodes;

CREATE TABLE nodes(
                      id BIGINT NOT NULL PRIMARY KEY,
                      username VARCHAR(256) NOT NULL,
                      longitude DOUBLE PRECISION NOT NULL,
                      latitude DOUBLE PRECISION NOT NULL
);

CREATE TABLE tags(
                     node_id BIGINT NOT NULL REFERENCES nodes(id),
                     key VARCHAR(256) NOT NULL,
                     value VARCHAR(256) NOT NULL,
                     PRIMARY KEY (node_id, key)
);