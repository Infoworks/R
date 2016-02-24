#Prepares the base table for phase 1
prepare_p1_ClaimBaseTableDF <- function() {
  
  #( select *
  #   from V_PHMCY_CLM
  #   join V_MBR_ACCT_HIST  ON MBR_ACCT_GID = MBR_ACCT_GID
  #   join V_DRUG_DENORM ON DRUG_PROD_GID = DRUG_PROD_GID
  #   where 	V_PHMCY_CLM.FILL_DT > (NOW() - 6 months)
  #   AND V_PHMCY_CLM.COB_CD = 'PRMRY' AND V_MBR_ACCT.EPH_LINK_ID IS NOT NULL AND V_MBR_ACCT.REC_SRC_IND = 'Y'
  #)  T0
  
  #load pharmacy claim (V_PHMCY_CLM)
  phmcyClm <- iwSelectTable(POC_MBR_OPPTY.V_PHMCY_CLM, "COB_CD = 'PRMRY' AND datediff(from_unixtime(unix_timestamp()), FILL_DT) < 180", "PHMCY")
  #load mbr acct hist (V_MBR_ACCT_HIST)
  mbrAcctHist <- iwSelectTable(POC_MBR_OPPTY.V_MBR_ACCT_HIST, "EPH_LINK_ID IS NOT NULL AND EPH_LINK_ID != 0 AND REC_SRC_FLG = 'Y'", "MBR_ACC")
  #load drug denorm
  drugDenorm <- iwSelectTable(POC_MBR_OPPTY.V_DRUG_DENORM, colPrefix="DRUG")
  
  #join (mbr acct and phmcy clm) and drug denorm
  joinedTable <- iwJoinTables(
    drugDenorm, iwJoinTables(phmcyClm, mbrAcctHist, "PHMCY_MBR_ACCT_GID = MBR_ACC_MBR_ACCT_GID"),
    "PHMCY_DRUG_PROD_GID = DRUG_DRUG_PROD_GID")

  return (joinedTable)
}

prepare_p1_maxFillDtTable <- function(claimBaseTable){
  #( select EPH_LINK_ID, GPI4_CD, MAX(FILL_DT) as max_fill_dt
  #   FROM T0
  #   GROUP BY EPH_LINK_ID, GPI4_CD
  #)  T1
  maxFillDtTable <- iwGroupBy(claimBaseTable, "MBR_ACC_EPH_LINK_ID, DRUG_GPI4_CD", "MFDT", "MAX(PHMCY_FILL_DT) AS MAX_PHMCY_FILL_DT")
  return (maxFillDtTable)
}

prepare_p1_maxClmEventGid <- function(claimBaseTable) {
  
  #( select EPH_LINK_ID, GPI4_CD, T1.max_fill_dt, MAX(T0.CLM_EVNT_GID) as max_clm_evnt_gid
  #   FROM T0
  #   JOIN T1 ON T0.EPH_LINK_ID = T1.EPH_LINK_ID AND T0.GPI4_CD = T1.GPI4_CD AND T0.FILL_DT = T1.max_fill_dt
  #   GROUP BY EPH_LINK_ID, GPI4_CD , T1.max_fill_dt
  #)  T2
  # T1 is maxFillDtTable
  
  maxFillDtTable <- prepare_p1_maxFillDtTable(claimBaseTable)

  #join the tables ON T0.EPH_LINK_ID = T1.EPH_LINK_ID AND T0.GPI4_CD = T1.GPI4_CD AND T0.FILL_DT = T1.max_fill_dt  
  maxClmEventGidTable <- iwJoinTables(claimBaseTable, maxFillDtTable,
     "MBR_ACC_EPH_LINK_ID = MFDT_MBR_ACC_EPH_LINK_ID AND DRUG_GPI4_CD = MFDT_DRUG_GPI4_CD AND PHMCY_FILL_DT = MFDT_MAX_PHMCY_FILL_DT")

  #group by EPH_LINK_ID, GPI4_CD , T1.max_fill_dt
  #select EPH_LINK_ID, GPI4_CD, T1.max_fill_dt, MAX(T0.CLM_EVNT_GID) as max_clm_evnt_gid
  maxClmEventGidTable <- iwGroupBy(maxClmEventGidTable, 
    "MBR_ACC_EPH_LINK_ID, DRUG_GPI4_CD, PHMCY_FILL_DT", "MCEGT",
    "MAX(PHMCY_CLM_EVNT_GID) AS MAX_CLM_EVNT_GID")

  return (maxClmEventGidTable)
}

prepare_p1_LatestClaimTable <- function()  {
  
  claimBaseTable <- prepare_p1_ClaimBaseTableDF()
  maxClmEventGidTable <- prepare_p1_maxClmEventGid(claimBaseTable)
  
  #(
  #select T0.*
  #  FROM T0
  #  JOIN T2 ON T0.MBR_ACC_EPH_LINK_ID = T2.MBR_ACC_EPH_LINK_ID AND T0.DRUG_GPI4_CD = T2.DRUG_GPI4_CD AND T0.PHMCY_FILL_DT = T2.PHMCY_FILL_DT AND T0.MAX_CLM_EVNT_GID = T2.MAX_CLM_EVNT_GID
  #) latestClaimTable
  # T0 = claimBaseTable  and T2 = maxClmEventGidTable
  
  latestClaimBaseTable <- iwJoinTables(claimBaseTable, maxClmEventGidTable,
     "MBR_ACC_EPH_LINK_ID = MCEGT_MBR_ACC_EPH_LINK_ID AND DRUG_GPI4_CD = MCEGT_DRUG_GPI4_CD AND PHMCY_FILL_DT = MCEGT_PHMCY_FILL_DT AND PHMCY_CLM_EVNT_GID = MCEGT_MAX_CLM_EVNT_GID",
     selectCols = names(claimBaseTable))

  return (latestClaimBaseTable)
}

prepare_p2_table <- function(iwdfs) {
  
  latestClaimTable <- prepare_p1_LatestClaimTable()
  
}