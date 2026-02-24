CREATE TABLE DimDate (
	dateid INT,
	date DATE NOT NULL,
	year INT NOT NULL,
	quarter SMALLINT NOT NULL,
	quartername VARCHAR(2) NOT NULL,
	month SMALLINT NOT NULL,
	monthname VARCHAR(9) NOT NULL,
	day SMALLINT NOT NULL,
	weekday SMALLINT NOT NULL,
	weekdayname VARCHAR(9) NOT NULL,
	PRIMARY KEY (dateid)
);


CREATE TABLE DimProduct
(
	productid INTEGER,
	producttype VARCHAR(255) NOT NULL,
	PRIMARY KEY (productid)
);


CREATE TABLE DimCustomerSegment
(
	segmentid INTEGER,
	city VARCHAR(255) NOT NULL,
	PRIMARY KEY (segmentid)
);


CREATE TABLE FactSales
(
	salesid VARCHAR(255),
	dateid INTEGER NOT NULL REFERENCES DimDate(dateid),
	productid INTEGER NOT NULL REFERENCES DimProduct(productid),
	segmentid INTEGER NOT NULL REFERENCES DimCustomerSegment(segmentid),
	priceperunit NUMERIC(19, 2) NOT NULL,
	quantitysold INTEGER NOT NULL,
	PRIMARY KEY (salesid)
);
