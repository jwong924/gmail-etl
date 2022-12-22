CREATE USER IF NOT EXISTS 'auth_user'@'localhost' IDENTIFIED BY 'Auth$123';

CREATE USER IF NOT EXISTS 'gmail_user'@'localhost' IDENTIFIED BY 'Gmail$123';

CREATE IF NOT EXISTS DATABASE auth;

CREATE IF NOT EXISTS DATABASE gmail;

GRANT ALL PRIVILEGES ON auth.* TO 'auth_user'@'localhost' ;

GRANT ALL PRIVILEGES ON gmail.* TO 'gmail_user'@'localhost' ;

USE auth;

CREATE TABLE IF NOT EXISTS user (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    password VARCHAR(255) NOT NULL
);

INSERT INTO user (username, password) VALUES ('test','Admin$123');

USE gmail;

CREATE TABLE IF NOT EXISTS email (
    id VARCHAR(255) NOT NULL PRIMARY KEY,
    date date
);