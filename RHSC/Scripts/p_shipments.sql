CREATE OR REPLACE VIEW p_shipments AS 
SELECT
	`v_dat_shipments`.`parentID`                                      AS `parentID`
	,`v_dat_shipments`.`l1`                                             AS `l1`
	,`v_dat_shipments`.`l2`                                             AS `l2`
	,`v_dat_shipments`.`l3`                                             AS `l3`
	,`v_dat_shipments`.`l4`                                             AS `l4`
	,`v_dat_shipments`.`l5`                                             AS `l5`
	,`v_dat_shipments`.`Region_Name`                                    AS `Region_Name`
	,`v_dat_shipments`.`Sub_region_Name`                                AS `Sub_region_Name`
	,`v_dat_shipments`.`Intermediate_Region_Name`                       AS `Intermediate_Region_Name`
	,`v_dat_shipments`.`Shipment_ID`                                    AS `Shipment_ID`
	,`v_dat_shipments`.`Country_ISO_Code`                               AS `Country_ISO_Code`
	,`v_dat_shipments`.`Country_Name`                                   AS `Country_Name`
	,`v_dat_shipments`.`Shipment_Creation_Date`                         AS `Shipment_Creation_Date`
	,`v_dat_shipments`.`Shipment_Creator_Code`                          AS `Shipment_Creator_Code`
	,`v_dat_shipments`.`Estimated_Ship_Date`                            AS `Estimated_Shipment_Date`
	,`v_dat_shipments`.`Estimated_Delivery_Date`                        AS `Estimated_Delivery_Date`
	,`v_dat_shipments`.`Funder`                                         AS `Funder Original`
	,`v_dat_shipments`.`Actual_Ship_Date`                               AS `Actual_Ship_Date`
	,`v_dat_shipments`.`Procurer`                                       AS `Procurer`
	,`v_dat_shipments`.`Shipped_Qty`                                    AS `Shipped_Qty`
	,`v_dat_shipments`.Base_UOM                                         AS `Base_UOM`
	,`v_dat_shipments`.`L3_Method`                                      AS `L3_Method`
	,`v_dat_shipments`.`Delivered_Date`                                 AS `Delivered_Date`
	,`v_dat_shipments`.`Received_Date`                                  AS `Received_Date`
	,`v_dat_shipments`.`Delivered_Qty`                                  AS `Delivered_Qty`
	,`v_dat_shipments`.`Received_Qty`                                   AS `Received_Qty`
	,`v_dat_shipments`.`Estimated_Unit_Value_USD`                       AS `Estimated_Unit_Value_USD`
	,`v_dat_shipments`.`Estimated_Line_Value_USD`                       AS `Estimated_Line_Value_USD`
	,`v_dat_shipments`.`Shipment_Line_Status`                           AS `Shipment_Line_Status`
	, md_funder_mapping.VAN_Funder                                      AS `Funder`                  
  , now()                                                           AS `Updated_At`           
    	
FROM
	`rhsc_gfpvan`.`v_dat_shipments`
LEFT JOIN md_funder_mapping on v_dat_shipments.Funder = md_funder_mapping.Source_Funder
	 
UNION
SELECT
	''                                                AS `parentID`
	,`rhsc_gfpvan`.`arc_rhi`.`L1`                       AS `l1`
	,`rhsc_gfpvan`.`arc_rhi`.`L2`                       AS `l2`
	,`rhsc_gfpvan`.`arc_rhi`.`L3`                       AS `l3`
	,`rhsc_gfpvan`.`arc_rhi`.`L4`                       AS `l4`
	,`rhsc_gfpvan`.`arc_rhi`.`L5`                       AS `l5`
	,`rhsc_gfpvan`.`arc_rhi`.`Region_Name`              AS `Region_Name`
	,`rhsc_gfpvan`.`arc_rhi`.`Sub_region_name`          AS `Sub_region_Name`
	,`rhsc_gfpvan`.`arc_rhi`.`Intermediate_Region_Name` AS `Intermediate_Region_Name`
	,`rhsc_gfpvan`.`arc_rhi`.`Shipment_ID`              AS `Shipment_ID`
	,`rhsc_gfpvan`.`arc_rhi`.`Country_ISO_Code`         AS `Country_ISO_Code`
	,`rhsc_gfpvan`.`arc_rhi`.`Country_Name`             AS `Country_Name`
	,`rhsc_gfpvan`.`arc_rhi`.`Shipment_Date`            AS `Shipment_Creation_Date`
	,`rhsc_gfpvan`.`arc_rhi`.`Shipment_Creator_Code`    AS `Shipment_Creator_Code`
    ,`rhsc_gfpvan`.`arc_rhi`.`Shipment_Date`            AS `Estimated_Shipment_Date`
	,`rhsc_gfpvan`.`arc_rhi`.`Received_Date`            AS `Estimated_Delivery_Date`
	,`rhsc_gfpvan`.`arc_rhi`.`Funder`                   AS `Funder Original`
	,`rhsc_gfpvan`.`arc_rhi`.`Shipment_Date`            AS `Actual_Ship_Date`
	,`rhsc_gfpvan`.`arc_rhi`.`Procurer`                 AS `Procurer`
	,`rhsc_gfpvan`.`arc_rhi`.`Shipped_Qty`              AS `Shipped_Qty`
	,''                                                 AS `UOM`
	,`rhsc_gfpvan`.`arc_rhi`.`L3`                       AS `L3_Method`
	,`rhsc_gfpvan`.`arc_rhi`.`Received_Date`            AS `Delivered_Date`
	,`rhsc_gfpvan`.`arc_rhi`.`Received_Date`            AS `Received_Date`
	,`rhsc_gfpvan`.`arc_rhi`.`Shipped_Qty`              AS `Delivered_Qty`
	,`rhsc_gfpvan`.`arc_rhi`.`Shipped_Qty`              AS `Received_Qty`
	,`rhsc_gfpvan`.`arc_rhi`.`Unit_Price`               AS `Estimated_Unit_Value_USD`
	,`rhsc_gfpvan`.`arc_rhi`.`Item_Total_Price`         AS `Estimated_Line_Value_USD`
	,'Shipped'                                          AS `Shipment_Line_Status`
	, md_funder_mapping.VAN_Funder                      AS `Funder`                 
  , now()                                             AS `Updated_At`             
FROM
	`rhsc_gfpvan`.`arc_rhi`
LEFT JOIN md_funder_mapping on arc_rhi.Funder = md_funder_mapping.Source_Funder
WHERE
    arc_rhi.Order_Status = 'Shipped'                                             
    and arc_rhi.Received_Date < '2017-01-01'