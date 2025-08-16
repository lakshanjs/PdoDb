-- Create a scratch database (change the name if you like)
CREATE DATABASE IF NOT EXISTS pdodb_test
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;
USE pdodb_test;

-- Users
CREATE TABLE users (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(191) NOT NULL UNIQUE,
    login VARCHAR(100) NOT NULL,
    role ENUM('user','manager','admin') NOT NULL DEFAULT 'user',
    status ENUM('active','inactive','banned') NOT NULL DEFAULT 'active',
    views INT UNSIGNED NOT NULL DEFAULT 0,
    quota INT NOT NULL DEFAULT 100,
    note TINYINT(1) NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_login DATETIME NULL
) ENGINE=InnoDB;

-- Profiles
CREATE TABLE profiles (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    user_id INT UNSIGNED NOT NULL,
    full_name VARCHAR(191) NOT NULL,
    active TINYINT(1) NOT NULL DEFAULT 1,
    CONSTRAINT fk_profiles_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    INDEX (user_id),
    INDEX (active)
) ENGINE=InnoDB;

-- Posts
CREATE TABLE posts (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    user_id INT UNSIGNED NOT NULL,
    title VARCHAR(191) NOT NULL,
    body TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_posts_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    INDEX (user_id),
    INDEX (created_at)
) ENGINE=InnoDB;

-- Tags
CREATE TABLE tags (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
) ENGINE=InnoDB;

-- Post <-> Tag (many-to-many)
CREATE TABLE post_tags (
    post_id INT UNSIGNED NOT NULL,
    tag_id  INT UNSIGNED NOT NULL,
    PRIMARY KEY (post_id, tag_id),
    CONSTRAINT fk_pt_post FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE,
    CONSTRAINT fk_pt_tag  FOREIGN KEY (tag_id)  REFERENCES tags(id)  ON DELETE CASCADE
) ENGINE=InnoDB;

-- Sessions (for delete demo)
CREATE TABLE sessions (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    user_id INT UNSIGNED NOT NULL,
    last_seen DATETIME NOT NULL,
    INDEX (user_id),
    INDEX (last_seen)
) ENGINE=InnoDB;

-- Log (for transaction demo)
CREATE TABLE log (
    id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    msg VARCHAR(255) NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- Seed data
INSERT INTO users (email, login, role, status, views, quota, note, created_at, last_login) VALUES
('admin@example.com',  'admin',  'admin',   'active',   10, 100, 0, NOW(), NOW()),
('manager@example.com','manager','manager', 'active',    5, 100, 0, NOW(), NULL),
('user1@example.com',  'user1',  'user',    'inactive',  0, 100, 0, NOW(), NULL),
('user2@example.com',  'user2',  'user',    'active',    2, 100, 0, NOW(), NULL);

INSERT INTO profiles (user_id, full_name, active) VALUES
(1, 'Site Admin', 1),
(2, 'Jane Manager', 1),
(3, 'User One', 0),
(4, 'User Two', 1);

INSERT INTO posts (user_id, title, body) VALUES
(1, 'Welcome', 'First post'),
(2, 'Roadmap', 'Q3 goals'),
(4, 'Hello', 'Intro text');

INSERT INTO tags (name) VALUES ('php'),('pdo'),('security');

INSERT INTO post_tags (post_id, tag_id)
SELECT p.id, t.id FROM posts p JOIN tags t ON t.name IN ('php','pdo');

INSERT INTO sessions (user_id, last_seen) VALUES
(1, NOW() - INTERVAL 10 DAY),
(1, NOW() - INTERVAL 1 DAY),
(2, NOW() - INTERVAL 400 DAY),
(4, NOW());
