-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS bigdataassignment

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS clinicaltrial_2021
USING csv 
OPTIONS (
    path "/FileStore/tables/clinicaltrial_2021.csv",
    header "true",
    inferSchema "true",
    delimiter "|"
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS pharma
USING csv 
OPTIONS (
    path "/FileStore/tables/pharma.csv",
    header "true",
    inferSchema "true"
)

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

CREATE OR REPLACE TABLE bigdataassignment.clinicaltrial_2021 AS SELECT * FROM clinicaltrial_2021

-- COMMAND ----------

CREATE OR REPLACE TABLE bigdataassignment.pharma AS SELECT * FROM pharma

-- COMMAND ----------

SHOW TABLES in bigdataassignment

-- COMMAND ----------

SELECT COUNT(DISTINCT Id) AS Total_Count FROM bigdataassignment.clinicaltrial_2021

-- COMMAND ----------

CREATE OR REPLACE VIEW bigdataassignment.getFrequencyType AS 
SELECT Type,Count(Type) AS Frequency
FROM bigdataassignment.clinicaltrial_2021
GROUP BY Type
ORDER BY Count(Type) DESC

-- COMMAND ----------

SELECT * from bigdataassignment.getFrequencyType

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW Split_Conditions AS SELECT `Id`, split(Conditions, ',') AS Split_Conditions FROM bigdataassignment.clinicaltrial_2021

-- COMMAND ----------

Select * from Split_Conditions

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW Explode_Conditions AS SELECT `Id`, explode(Split_Conditions) AS Conditions FROM Split_Conditions

-- COMMAND ----------

Select * from Explode_Conditions

-- COMMAND ----------

SELECT Conditions, count(*) AS Condition_count FROM Explode_Conditions
WHERE Conditions IS NOT NULL AND LENGTH(Conditions) > 0
GROUP BY Conditions
ORDER BY Condition_count DESC LIMIT (5)

-- COMMAND ----------

CREATE OR REPLACE VIEW bigdataassignment.commonSponsors AS 
SELECT Sponsor, Count(Sponsor) AS CountSponsor
FROM bigdataassignment.clinicaltrial_2021
WHERE Sponsor NOT IN (SELECT Company FROM pharma)
GROUP BY Sponsor
ORDER BY Count(Sponsor) DESC
LIMIT (10)

-- COMMAND ----------

SELECT * from bigdataassignment.commonSponsors

-- COMMAND ----------

Select SUBSTR(Completion,1,4) AS MONTH,Completion, Count(Completion) AS `TOTAL COUNT`  
from bigdataassignment.clinicaltrial_2021
WHERE Completion LIKE '%2021%' AND Status = 'Completed'
GROUP BY Completion
ORDER BY MONTH(CAST(TO_DATE(Completion,'MMM yyyy') AS DATE))

-- COMMAND ----------

CREATE OR REPLACE VIEW bigdataassignment.OffenseList AS 
SELECT Offense_Group, Count(Offense_Group) AS CountOffenseGroup
FROM bigdataassignment.pharma
WHERE Company NOT IN (SELECT Sponsor FROM bigdataassignment.clinicaltrial_2021)
GROUP BY Offense_Group
ORDER BY Count(Offense_Group) DESC
LIMIT (10)

-- COMMAND ----------

SELECT * from bigdataassignment.OffenseList

-- COMMAND ----------

CREATE OR REPLACE VIEW bigdataassignment.getUniversityList AS 
SELECT Sponsor,Status,Conditions FROM bigdataassignment.clinicaltrial_2021
WHERE Sponsor LIKE '%University%' AND Type ='Observational [Patient Registry]' AND Conditions IS NOT NULL

-- COMMAND ----------

SELECT * FROM bigdataassignment.getUniversityList
