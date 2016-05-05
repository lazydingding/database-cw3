DROP TABLE IF EXISTS LikePost;
DROP TABLE IF EXISTS LikeTopic;
DROP TABLE IF EXISTS FavTopic;
DROP TABLE IF EXISTS Post;
DROP TABLE IF EXISTS Topic;
DROP TABLE IF EXISTS Forum;
DROP TABLE IF EXISTS Person;


CREATE TABLE Person (
   id INTEGER PRIMARY KEY,
   name VARCHAR(100) NOT NULL,
   username VARCHAR(10) NOT NULL UNIQUE,
   stuId VARCHAR(10) NULL
);

CREATE TABLE Forum (
   id INTEGER PRIMARY KEY,
   title VARCHAR(100) NOT NULL
);

CREATE TABLE Topic (
   id INTEGER PRIMARY KEY,
   title VARCHAR(100) NOT NULL,
   topictext VARCHAR(200) NOT NULL,
   forum INTEGER REFERENCES Forum(id),
   creator INTEGER REFERENCES Person(id),
   created INTEGER NOT NULL
);

CREATE TABLE Post (
   num INTEGER NOT NULL,
   topic INTEGER REFERENCES Topic(id),
   author INTEGER REFERENCES Person(id),
   content TEXT NOT NULL,
   created INTEGER NOT NULL,
   PRIMARY KEY(num, topic)
);

CREATE TABLE LikePost (
   person INTEGER REFERENCES Person(id),
   postnum INTEGER,
   postopic INTEGER,
   FOREIGN KEY(postnum, postopic) REFERENCES Post(num, topic)
);

CREATE TABLE LikeTopic (
   person INTEGER REFERENCES Person(id),
   topic INTEGER REFERENCES Topic(id)
);

CREATE TABLE FavTopic (
   person INTEGER REFERENCES Person(id),
   topic INTEGER REFERENCES Topic(id)
);

INSERT INTO Person (id, name, username, stuId) VALUES (10001, 'Luping Yu', 'Jack', 'ly15516');
INSERT INTO Person (id, name, username, stuId) VALUES (10002, 'Khas', 'Xac', 'ks15894');
INSERT INTO Person (id, name, username, stuId) VALUES (10003, 'Fan Zhao', 'Emma', 'fz15284');
INSERT INTO Forum (id, title) VALUES (101, 'Database');
INSERT INTO Forum (id, title) VALUES (102, 'Java');
INSERT INTO Forum (id, title) VALUES (103, 'Research Skills');
INSERT INTO Topic (id, title, topictext, forum, creator, created) VALUES (201, 'Course Work 1', 'Hello world', 101, 10001, 1315);
INSERT INTO Topic (id, title, topictext, forum, creator, created) VALUES (202, 'Course Work 2', 'Hello world', 101, 10002, 1430);
INSERT INTO Topic (id, title, topictext, forum, creator, created) VALUES (203, 'Java Swing', 'Hello world', 102, 10003, 1545);
INSERT INTO Post (num, topic, author, content, created) VALUES (1, 201, 10001, 'May I ask you a question?', 1807);
INSERT INTO Post (num, topic, author, content, created) VALUES (2, 201, 10001, 'Of course you can.', 1808);
INSERT INTO Post (num, topic, author, content, created) VALUES (3, 201, 10003, 'Cool!', 1809);
INSERT INTO Post (num, topic, author, content, created) VALUES (1, 202, 10001, 'Hello World!', 1801);
INSERT INTO Post (num, topic, author, content, created) VALUES (2, 202, 10002, 'How are you?', 1802);
INSERT INTO Post (num, topic, author, content, created) VALUES (3, 202, 10003, 'I am fine, thanks. And you?', 1803);
INSERT INTO Post (num, topic, author, content, created) VALUES (4, 202, 10001, 'I am OK.', 1804);
INSERT INTO Post (num, topic, author, content, created) VALUES (5, 202, 10002, 'Alright, enjoy your homework!', 1805);
INSERT INTO Post (num, topic, author, content, created) VALUES (6, 202, 10003, 'Thanks, you too.', 1806);
INSERT INTO Post (num, topic, author, content, created) VALUES (1, 203, 10001, 'Can not be empty!', 1810);
INSERT INTO LikePost (person, postnum, postopic) VALUES (10001, 1, 202);
INSERT INTO LikePost (person, postnum, postopic) VALUES (10001, 2, 202);
INSERT INTO LikePost (person, postnum, postopic) VALUES (10001, 3, 202);
INSERT INTO LikePost (person, postnum, postopic) VALUES (10001, 4, 202);
INSERT INTO LikePost (person, postnum, postopic) VALUES (10002, 1, 202);
INSERT INTO LikePost (person, postnum, postopic) VALUES (10003, 1, 202);
INSERT INTO LikePost (person, postnum, postopic) VALUES (10001, 1, 201);
INSERT INTO LikeTopic (person, topic) VALUES (10001, 201);
INSERT INTO LikeTopic (person, topic) VALUES (10002, 201);
INSERT INTO LikeTopic (person, topic) VALUES (10001, 202);
INSERT INTO FavTopic (person, topic) VALUES (10001, 201);
INSERT INTO FavTopic (person, topic) VALUES (10001, 202);
INSERT INTO FavTopic (person, topic) VALUES (10002, 202);
