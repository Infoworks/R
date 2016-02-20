transform_NORMALIZED_DAY_SPLY_QTY <- function(table) {
  #Transformation for NORMALIZED_DAY_SPLY_QTY
  mutatedDF <- selectExpr(table, "(CASE WHEN DAY_SPLY_QTY BETWEEN 1 AND 33  THEN 30 WHEN DAY_SPLY_QTY BETWEEN 34 AND 83 THEN  60 WHEN DAY_SPLY_QTY >=84 THEN 90 END) as NORMALIZED_DAY_SPLY_QTY")
  return (mutatedDF)
}


#Prepares the base table for phase 1
prepare_p1_ClaimBaseTableDF <- function(iwdfs) {

  #( select *
  #   from V_PHMCY_CLM
  #   join V_MBR_ACCT_HIST  ON MBR_ACCT_GID = MBR_ACCT_GID
  #   join V_DRUG_DENORM ON DRUG_PROD_GID = DRUG_PROD_GID
  #   where 	V_PHMCY_CLM.FILL_DT > (NOW() - 6 months)
  #   AND V_PHMCY_CLM.COB_CD = 'PRMRY'
  #   AND V_MBR_ACCT.EPH_LINK_ID IS NOT NULL
  #)  T0


  #load pharmacy claim
  iwdfs.V_PHMCY_CLM <- SelectDF(POC_MBR_OPPTY.V_PHMCY_CLM)
  iwdfs.V_PHMCY_CLM <- aliasDF(iwdfs.V_PHMCY_CLM, "PHMCY", TRUE)

  #add filters
  #COB_CD = 'PRMRY'
  iwdfs.V_PHMCY_CLM <- filter(iwdfs.V_PHMCY_CLM,   iwdfs.V_PHMCY_CLM$PHMCY_COB_CD != "PRMY")
  #apply filter for last 3 months
  iwdfs.V_PHMCY_CLM <- filter(iwdfs.V_PHMCY_CLM, "datediff(from_unixtime(unix_timestamp()), PHMCY_FILL_DT) < 180")

  #load mbr acct
  iwdfs.V_MBR_ACCT_HIST <- SelectDF(POC_MBR_OPPTY.V_MBR_ACCT_HIST)
  iwdfs.V_MBR_ACCT_HIST <- aliasDF(iwdfs.V_MBR_ACCT_HIST, "MBR_ACC", TRUE)
  #add non null filter
  iwdfs.V_MBR_ACCT_HIST <- filter(iwdfs.V_MBR_ACCT_HIST, isNotNull(iwdfs.V_MBR_ACCT_HIST$MBR_ACC_EPH_LINK_ID))

  #load drug denorm
  iwdfs.V_DRUG_DENORM <- SelectDF(POC_MBR_OPPTY.V_DRUG_DENORM)
  iwdfs.V_DRUG_DENORM <- aliasDF(iwdfs.V_DRUG_DENORM, "DRUG", TRUE)


  #join mbr acct and phmcy clm
  joinedDf <- join(iwdfs.V_PHMCY_CLM, iwdfs.V_MBR_ACCT_HIST,
  iwdfs.V_PHMCY_CLM$PHMCY_MBR_ACCT_GID == iwdfs.V_MBR_ACCT_HIST$MBR_ACC_MBR_ACCT_GID)
  #join (mbr acct and phmcy clm) and drug
  joinedDf <- join(joinedDf, iwdfs.V_DRUG_DENORM,
  joinedDf$PHMCY_DRUG_PROD_GID == iwdfs.V_DRUG_DENORM$DRUG_DRUG_PROD_GID)

  return (joinedDf)
}

prepare_p1_maxFillDtTable <- function(claimBaseTable){
  #( select EPH_LINK_ID, GPI4_CD, MAX(FILL_DT) as max_fill_dt
  #   FROM T0
  #   GROUP BY EPH_LINK_ID, GPI4_CD
  #)  T1

  #group by EPH_LINK_ID, GPI4_CD
  maxFillDtTable <- groupBy(claimBaseTable, claimBaseTable$MBR_ACC_EPH_LINK_ID, claimBaseTable$DRUG_GPI4_CD)
  #select EPH_LINK_ID, GPI4_CD, MAX(FILL_DT) as max_fill_dt
  maxFillDtTable <- agg(maxFillDtTable, MAX_PHMCY_FILL_DT = max(claimBaseTable$PHMCY_FILL_DT))

  maxFillDtTable <- aliasDF(maxFillDtTable, "MFDT", TRUE)

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


  #join the tables
  # T0.EPH_LINK_ID = T1.EPH_LINK_ID AND T0.GPI4_CD = T1.GPI4_CD AND T0.FILL_DT = T1.max_fill_dt
  maxClmEventGidTable <- join(
  claimBaseTable, maxFillDtTable,
  claimBaseTable$MBR_ACC_EPH_LINK_ID == maxFillDtTable$MFDT_MBR_ACC_EPH_LINK_ID
  & claimBaseTable$DRUG_GPI4_CD == maxFillDtTable$MFDT_DRUG_GPI4_CD
  & claimBaseTable$PHMCY_FILL_DT == maxFillDtTable$MFDT_MAX_PHMCY_FILL_DT)

  #group by EPH_LINK_ID, GPI4_CD , T1.max_fill_dt
  maxClmEventGidTableGrp <- groupBy(maxClmEventGidTable,
  maxClmEventGidTable$MBR_ACC_EPH_LINK_ID, maxClmEventGidTable$DRUG_GPI4_CD, maxClmEventGidTable$PHMCY_FILL_DT)

  #select EPH_LINK_ID, GPI4_CD, T1.max_fill_dt, MAX(T0.CLM_EVNT_GID) as max_clm_evnt_gid
  maxClmEventGidTable <- agg(maxClmEventGidTableGrp,
  MAX_CLM_EVNT_GID = max(maxClmEventGidTable$PHMCY_CLM_EVNT_GID) )

  maxClmEventGidTable <- aliasDF(maxClmEventGidTable, "MCEGT", TRUE)

  return (maxClmEventGidTable)
}


prepare_p1_LatestClaimTable <- function(iwdfs)  {

  claimBaseTable <- prepare_p1_ClaimBaseTableDF(iwdfs)

  maxClmEventGidTable <- prepare_p1_maxClmEventGid(claimBaseTable)


  #(
  #select T0.*
  #  FROM T0
  #  JOIN T2 ON T0.MBR_ACC_EPH_LINK_ID = T2.MBR_ACC_EPH_LINK_ID AND T0.DRUG_GPI4_CD = T2.DRUG_GPI4_CD AND T0.PHMCY_FILL_DT = T2.PHMCY_FILL_DT AND T0.MAX_CLM_EVNT_GID = T2.MAX_CLM_EVNT_GID
  #) latestClaimTable
  # T0 = claimBaseTable  and T2 = maxClmEventGidTable

  latestClaimBaseTable <- join(claimBaseTable, maxClmEventGidTable,
  claimBaseTable$MBR_ACC_EPH_LINK_ID == maxClmEventGidTable$MCEGT_MBR_ACC_EPH_LINK_ID & claimBaseTable$DRUG_GPI4_CD == maxClmEventGidTable$MCEGT_DRUG_GPI4_CD & claimBaseTable$PHMCY_FILL_DT == maxClmEventGidTable$MCEGT_PHMCY_FILL_DT & claimBaseTable$PHMCY_CLM_EVNT_GID == maxClmEventGidTable$MCEGT_MAX_CLM_EVNT_GID)

  #Select T0.*
  col_names <- lapply(names(claimBaseTable), function(x) x)
  finalLatestClaimTable <- select(latestClaimBaseTable, col_names)

  print("VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV")
  print(names(finalLatestClaimTable))
  showDF(finalLatestClaimTable)
  print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")

  return (finalLatestClaimTable)
}
