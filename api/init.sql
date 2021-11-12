CREATE TABLE dogdata
(
	id SERIAL PRIMARY KEY,
	name varchar(50) NOT NULL UNIQUE,
	breed varchar(50) NOT NULL
);
