

--table clean up
drop table features_scaled;
drop table KMeans_Model;
delete from clusters; 


-- Create base table to hold source data. Data must be copied or loaded to this table. This SQL does not perform the data loading.
CREATE TABLE features (
	entity_id integer NOT NULL,
	entity_name varchar(255) NOT NULL,
	special_facility SMALLINT,
	Exclude_Facility SMALLINT,
	measure_dt date,
	count_fte integer, 
	count_beds integer,
	count_admissions integer,
	score_case_mix decimal(3,2),
	sum_net_revenue bigint,
	sum_opex bigint
) PRIMARY index(entity_id);


--validate data was loaded correctly
select * from features;


--Create table to store Original Clustering
create table original_cluster (
	entity_id integer not null,
	cluster_id integer
);


--Create table to store Teradata Clustering
create table clusters (
	entity_id integer NOT NULL,
	cluster_id smallint not null,
	score_date DATE
);


--Create View to that can be used to filter out special or excluded facilities
REPLACE VIEW v_features AS
SELECT hf.*
FROM features hf
JOIN original_cluster hu ON hf.entity_id = hu.entity_id
WHERE hu.cluster_id IS NOT NULL
AND hf.Exclude_Facility < 1;

--add in above to also exclude special facilities
AND hf.special_facility < 1;



--Calculate Scaling parameters
SELECT * FROM TD_ScaleFit(
    ON v_features AS InputTable
    OUT TABLE OutputTable(features_scaled)
    USING
        TargetColumns('count_fte' , 'count_beds' ,'count_admissions' ,'score_case_mix','sum_net_revenue' ,'sum_opex')
        ScaleMethod('range')
        MissValue('zero')
) as dt;



-- return the data as scaled values between 0-1 
--   and join to the base data to capture any additional data
replace view v_features_scaled 
as
SELECT hf.entity_id, hf.special_facility, hs.count_fte, 
	hs.count_beds, hs.count_admissions, hs.score_case_mix, hs.sum_net_revenue, hs.sum_opex 
FROM TD_scaleTransform (
  ON v_features AS InputTable
  ON features_scaled AS FitTable DIMENSION
  USING
  Accumulate ('entity_id')
) AS hs
	JOIN v_features hf on (hf.entity_id = hs.entity_id);

--review scaled data 
select * from v_features_scaled;


--table cleanup
drop table KMeans_Model;

--perform KMeans Clustering
Select * from TD_KMeans (
    ON v_features_scaled as InputTable
    OUT TABLE ModelTable(KMeans_Model)
    USING
        IdColumn('entity_id')
        TargetColumns('special_facility', 'count_fte' , 'count_beds' ,'count_admissions' ,'score_case_mix','sum_net_revenue' ,'sum_opex')
        StopThreshold(0.001)
        NumClusters(6)
        --Seed(0)
        MaxIterNum(100)
)as dt;


--table cleanup
delete from clusters; 

--Create table to associate entity_id with assigned Teradata Clusters
Insert into clusters (entity_id, cluster_id, score_date)
    SELECT entity_id, td_clusterid_kmeans, CURRENT_DATE()  
    FROM TD_KMeansPredict (
        ON v_features_scaled AS InputTable
        ON KMeans_Model as ModelTable DIMENSION
        USING
            OutputDistance('false')
    ) as dt;


--Optional query to Join Original Clusters with Teradata
SELECT DISTINCT 
    hc.cluster_id AS Teradata_Cluster,
    original_cluster.cluster_id as Original_Cluster,
    hf.entity_id,      -- Include any other non-aggregated columns you want
    hf.entity_name,
    hf.count_admissions,
    hf.count_beds,
    hf.count_fte, 
    hf.sum_net_revenue, 
    hf.sum_opex,
    hf.special_facility,
    hf.Exclude_Facility 
 FROM 
    features hf
JOIN 
    clusters hc ON hf.entity_id = hc.entity_id
LEFT JOIN original_cluster
    ON hf.entity_id = original_cluster.entity_id;


--Optional query to calculate average values of Original Clusters vs Teradata
replace view v_original_stats 
as
Select cluster_id cluster_id, count(hf.entity_id) count_of_entities, avg(count_fte) avg_count_fte , 
	avg(count_beds) avg_count_beds, 
	avg(count_admissions) avg_count_admissions, 
	avg(score_case_mix) avg_score_case_mix, 
	avg(sum_net_revenue) avg_sum_net_revenue, 
	avg(sum_opex) avg_sum_opex
from features hf
	JOIN original_cluster hc on (hf.entity_id = hc.entity_id)
group by cluster_id;

--review output
select * from v_original_stats;


--Optional query to calculate average values of Teradata Clusters
replace view v_cluster_stats 
as
Select score_date, cluster_id, count(hf.entity_id) count_of_entities, avg(count_fte) avg_count_fte , 
	avg(count_beds) avg_count_beds, 
	avg(count_admissions) avg_count_admissions, 
	avg(score_case_mix) avg_score_case_mix, 
	avg(sum_net_revenue) avg_sum_net_revenue, 
	avg(sum_opex) avg_sum_opex
from features hf
	JOIN clusters hc on (hf.entity_id = hc.entity_id)
group by score_date, cluster_id;

--revivew output
select * from v_cluster_stats;


--Optional query to review Teradata Clusters
SELECT DISTINCT 
    hc.cluster_id as Teradata_Cluster,
    hf.entity_id,      -- Include any other non-aggregated columns you want
    hf.entity_name,
    hf.count_admissions,
    hf.count_beds,
    hf.count_fte, 
    hf.sum_net_revenue, 
    hf.sum_opex 
FROM 
    features hf
JOIN 
    clusters hc ON hf.entity_id = hc.entity_id;








