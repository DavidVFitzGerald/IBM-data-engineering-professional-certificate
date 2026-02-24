CREATE TABLE "MyDimDate" 
(
	dateid SERIAL,
	year SMALLINT NOT NULL,
	month SMALLINT NOT NULL,
	monthname VARCHAR(9) NOT NULL,
	day SMALLINT NOT NULL,
	weekday SMALLINT NOT NULL,
	weekdayname VARCHAR(9) NOT NULL,
	PRIMARY KEY (dateid)
);


CREATE TABLE "MyDimProduct"
(
	productid SERIAL,
	productname VARCHAR(255) NOT NULL,
	PRIMARY KEY (productid)
);


CREATE TABLE "MyDimCustomerSegment"
(
	segmentid SERIAL,
	segmentname VARCHAR(255) NOT NULL,
	PRIMARY KEY (segmentid)
);
	

CREATE TABLE "MyFactSales"
(
	salesid SERIAL,
	productid INTEGER NOT NULL REFERENCES "MyDimProduct"(productid),
	dateid INTEGER NOT NULL REFERENCES "MyDimDate"(dateid),
	segmentid INTEGER NOT NULL REFERENCES "MyDimCustomerSegment"(segmentid),
	quantitysold INTEGER NOT NULL,
	priceperunit NUMERIC(19, 2) NOT NULL
);

