create external table nypdComplaint (Zip_Codes string, CMPLNT_FR_DT date, CMPLNT_FR_TM string, CMPLNT_TO_DT date, CMPLNT_TO_TM string, CRM_ATPT_CPTD_CD string, LAW_CAT_CD string, BORO_NM string, LOC_OF_OCCUR_DESC string, PREM_TYP_DESC string, SUSP_AGE_GROUP string, SUSP_RACE string, SUSP_SEX string, VIC_AGE_GROUP string, VIC_RACE string, VIC_SEX string)
row format delimited fields terminated by ',(?=([^\"]*\"[^\"]*\")*[^\"]*$)'
location '/user/zy1190_nyu_edu/NYPD_complaint/data_cleansing/';

create external table nycproperty (Zip_Codes string, Borough string, Year date, StreetName string, HouseNo_Hi string, HouseNo_Lo string, CurrentMarketPrice double, CurrentActualPrice double)
row format delimited fields terminated by ', '
location '/user/zy1190_nyu_edu/NYC_property/';
