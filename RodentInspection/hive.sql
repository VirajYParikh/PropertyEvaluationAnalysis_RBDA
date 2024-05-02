create external table rodent_inspection_final (
    inspection_type string,
    job_ticket bigint,
    job_progress int,
    zipcode int,
    latitude string,
    longitude string,
    borough string,
    inspection_date date,
    result string,
    approved_date date
)
row format delimited fields terminated by ','
location '/user/vp2359_nyu_edu/RBDA_shared/RodentInspection_Tableau'
tblproperties("skip.header.line.count"="1");



create external table property_valuation (
   Zip_Codes int,
   Borough string, 
   Year date, 
   StreetName string,
   HouseNo_Hi string, 
   HouseNo_Lo string, 
   CurrentMarketPrice double, 
   CurrentActualPrice double
)
row format delimited fields terminated by ',(?=([^\"]\"[^\"]\")[^\"]$)'
location '/user/vp2359_nyu_edu/RBDA_shared/PropertyValuation/output_data_property3'
tblproperties("skip.header.line.count"="1");