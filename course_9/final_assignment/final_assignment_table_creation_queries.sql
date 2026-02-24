CREATE TABLE DimDate
(
	dateid INT PRIMARY KEY,
	date DATE NOT NULL,
	year INT NOT NULL,
	quarter SMALLINT NOT NULL,
	quartername VARCHAR(2) NOT NULL,
	month SMALLINT NOT NULL,
	monthname VARCHAR(9) NOT NULL,
	day SMALLINT NOT NULL,
	weekday SMALLINT NOT NULL,
	weekdayname VARCHAR(9) NOT NULL
);


CREATE TABLE DimTruck 
(
	truckid INT PRIMARY KEY,
	trucktype VARCHAR(255) NOT NULL
);


CREATE TABLE DimStation
(
	stationid INT PRIMARY KEY,
	city VARCHAR(255) NOT NULL
);


CREATE TABLE FactTrips
(
	tripid INT PRIMARY KEY,
	dateid INT NOT NULL REFERENCES DimDate(dateid),
	stationid INT NOT NULL REFERENCES DimStation(stationid),
	truckid INT NOT NULL REFERENCES DimTruck(truckid),
	wastecollected FLOAT(10) NOT NULL
);
