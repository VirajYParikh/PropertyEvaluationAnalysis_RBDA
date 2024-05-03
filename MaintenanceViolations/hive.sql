CREATE EXTERNAL TABLE maintenanceData (
    BoroID STRING,
    Borough STRING,
    Postcode STRING,
    Class STRING,
    CurrentStatusID STRING,
    CurrentStatus STRING,
    CurrentStatusDate STRING,
    NovType STRING,
    ViolationStatus STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/ap7989_nyu_edu/maintenanceData/';


CREATE EXTERNAL TABLE nycproperty (
    Zip_Codes STRING,
    Borough STRING,
    Year STRING,
    StreetName STRING,
    HouseNo_Hi STRING,
    HouseNo_Lo STRING,
    CurrentMarketPrice DOUBLE,
    CurrentActualPrice DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ', '
STORED AS TEXTFILE
LOCATION '/user/ap7989_nyu_edu/PropertyValuation/';

CREATE TABLE avg_property_evaluation (
    zip_codes STRING,
    avg_currentmarketprice DOUBLE
);

INSERT INTO TABLE avg_property_evaluation 
SELECT zip_codes, AVG(currentmarketprice) AS avg_currentmarketprice 
FROM nycproperty 
GROUP BY zip_codes;

CREATE TABLE zipcode_open_violations (
    postcode STRING,
    open_violations_count INT
);

INSERT INTO TABLE zipcode_open_violations
SELECT maintenancedata.postcode, COUNT(*) AS open_violations_count
FROM maintenancedata
WHERE maintenancedata.violationstatus = 'Open'
GROUP BY maintenancedata.postcode;

CREATE TABLE zipcode_borough_mapping (
    postcode STRING,
    borough STRING
);

INSERT INTO TABLE zipcode_borough_mapping
SELECT nycproperty.zip_codes, MIN(nycproperty.borough) AS borough
FROM nycproperty
GROUP BY nycproperty.zip_codes;
