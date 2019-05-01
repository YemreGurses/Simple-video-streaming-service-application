CREATE TABLE Customer(
customer_id SERIAL PRIMARY KEY NOT NULL,
email TEXT NOT NULL,
password TEXT NOT NULL,
first_name TEXT NOT NULL,
last_name TEXT NOT NULL,
session_count INT NOT NULL);

CREATE TABLE Plan(
plan_id SERIAL PRIMARY KEY NOT NULL,
name TEXT NOT NULL,
resolution TEXT NOT NULL,
max_parallel_session INT NOT NULL,
monthly_fee REAL NOT NULL);

CREATE TABLE Subscription(
s_id SERIAL PRIMARY KEY NOT NULL,
customer_id INT REFERENCES Customer(customer_id),
plan_id INT REFERENCES Plan(plan_id));

CREATE TABLE Watched(
w_id SERIAL PRIMARY KEY NOT NULL,
movie_id TEXT NOT NULL,
customer_id INT REFERENCES Customer(customer_id),
time DATE NOT NULL);

INSERT INTO Customer VALUES(DEFAULT, 'gy@hotmail.com', 'pass123', 'Yemre', 'Gurses', 0);
INSERT INTO Customer VALUES(DEFAULT, 'teoo@hotmail.com', 'kjas12', 'Huso', 'Teoman', 0);

INSERT INTO Plan VALUES(DEFAULT, 'Standart', 'FullHD', 1, 4);
INSERT INTO Plan VALUES(DEFAULT, 'Premium', 'FullHD', 2, 6);
INSERT INTO Plan VALUES(DEFAULT, 'Platinum', '2K/4K', 3, 10);

INSERT INTO Subscription VALUES(DEFAULT, 1, 2);
INSERT INTO Subscription VALUES(DEFAULT, 2, 3);

INSERT INTO Watched VALUES(DEFAULT, 'tt0001038', 1, '2018-07-07');
INSERT INTO Watched VALUES(DEFAULT, 'tt3460252', 1, '2018-09-08');
INSERT INTO Watched VALUES(DEFAULT, 'tt1853728', 2, '2019-02-20');