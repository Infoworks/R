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
  #   AND V_MBR_ACCT.REC_SRC_IND = 'Y'
  #)  T0


  #load pharmacy claim
  iwdfs.V_PHMCY_CLM <- SelectDF(POC_MBR_OPPTY.V_PHMCY_CLM)
  iwdfs.V_PHMCY_CLM <- aliasDF(iwdfs.V_PHMCY_CLM, "PHMCY", TRUE)
  #add filter COB_CD = 'PRMRY'
  iwdfs.V_PHMCY_CLM <- filter(iwdfs.V_PHMCY_CLM,   iwdfs.V_PHMCY_CLM$PHMCY_COB_CD == "PRMRY")
  #apply filter for last 3 months
  iwdfs.V_PHMCY_CLM <- filter(iwdfs.V_PHMCY_CLM, "datediff(from_unixtime(unix_timestamp()), PHMCY_FILL_DT) < 180")


  #load mbr acct
  iwdfs.V_MBR_ACCT_HIST <- SelectDF(POC_MBR_OPPTY.V_MBR_ACCT_HIST)
  iwdfs.V_MBR_ACCT_HIST <- aliasDF(iwdfs.V_MBR_ACCT_HIST, "MBR_ACC", TRUE)
  #add filters
  iwdfs.V_MBR_ACCT_HIST <- filter(iwdfs.V_MBR_ACCT_HIST,
      isNotNull(iwdfs.V_MBR_ACCT_HIST$MBR_ACC_EPH_LINK_ID)
      & iwdfs.V_MBR_ACCT_HIST$MBR_ACC_EPH_LINK_ID != 0
      & iwdfs.V_MBR_ACCT_HIST$MBR_ACC_REC_SRC_FLG == 'Y')


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
    claimBaseTable$MBR_ACC_EPH_LINK_ID == maxClmEventGidTable$MCEGT_MBR_ACC_EPH_LINK_ID
    & claimBaseTable$DRUG_GPI4_CD == maxClmEventGidTable$MCEGT_DRUG_GPI4_CD
    & claimBaseTable$PHMCY_FILL_DT == maxClmEventGidTable$MCEGT_PHMCY_FILL_DT
    & claimBaseTable$PHMCY_CLM_EVNT_GID == maxClmEventGidTable$MCEGT_MAX_CLM_EVNT_GID)

  #Select T0.*
  col_names <- lapply(names(claimBaseTable), function(x) x)
  finalLatestClaimTable <- select(latestClaimBaseTable, col_names)

  return (finalLatestClaimTable)
}




prepare_p2_table <- function(iwdfs) {

  latestClaimTable <- prepare_p1_LatestClaimTable(iwdfs)

  #Load MBR_ACCT_CVRG and join
  iwdfs.MBR_ACCT_CVRG <- SelectDF(POC_MBR_OPPTY.V_MBR_ACCT_CVRG)
  iwdfs.MBR_ACCT_CVRG <- aliasDF(iwdfs.MBR_ACCT_CVRG, "MBR_ACC_CVRG", TRUE)
  #join to table on MBR_ACCT_GID
  targetTable <- join(latestClaimTable, iwdfs.MBR_ACCT_CVRG,
  latestClaimTable$MBR_ACC_MBR_ACCT_GID == iwdfs.MBR_ACCT_CVRG$MBR_ACC_CVRG_MBR_ACCT_GID)


  #Load MBR_ACCT_CONT_CVRG
  iwdfs.MBR_ACCT_CONT_CVRG <- SelectDF(POC_MBR_OPPTY.V_MBR_ACCT_CONT_CVRG)
  iwdfs.MBR_ACCT_CONT_CVRG <- aliasDF(iwdfs.MBR_ACCT_CONT_CVRG, "MBR_ACC_CONT_CVRG", TRUE)
  #LEFT OUTER join to table on MBR_ACCT_GID
  targetTable <- join(targetTable, iwdfs.MBR_ACCT_CONT_CVRG,
    targetTable$MBR_ACC_MBR_ACCT_GID == iwdfs.MBR_ACCT_CONT_CVRG$MBR_ACC_CONT_CVRG_MBR_ACCT_GID,
    'left_outer')


  #Load V_CLNT_ACCT_DENORM
  iwdfs.CLNT_ACCT_DENORM <- SelectDF(POC_MBR_OPPTY.V_CLNT_ACCT_DENORM)
  iwdfs.CLNT_ACCT_DENORM <- aliasDF(iwdfs.CLNT_ACCT_DENORM, "CLNT_ACCT_DENORM", TRUE)
  #TODO Missing filter of CURR_IND = 'Y' ..  NOT REQUIRED
  #join to table on LVL1_ACCT_GID AND LVL3_ACCT_GID
  targetTable <- join(targetTable, iwdfs.CLNT_ACCT_DENORM,
    targetTable$PHMCY_LVL1_ACCT_GID == iwdfs.CLNT_ACCT_DENORM$CLNT_ACCT_DENORM_LVL1_ACCT_GID
    & targetTable$PHMCY_LVL3_ACCT_GID == iwdfs.CLNT_ACCT_DENORM$CLNT_ACCT_DENORM_LVL3_ACCT_GID)



  #Load V_PHMCY_DENORM
  iwdfs.PHMCY_DENORM <- SelectDF(POC_MBR_OPPTY.V_PHMCY_DENORM)
  iwdfs.PHMCY_DENORM <- aliasDF(iwdfs.PHMCY_DENORM, "PHMCY_DENORM", TRUE)
  #join to table on PHMCY_PTY_GID
  targetTable <- join(targetTable, iwdfs.PHMCY_DENORM,
    targetTable$PHMCY_PHMCY_PTY_GID == iwdfs.PHMCY_DENORM$PHMCY_DENORM_PHMCY_PTY_GID)


  #TODO MISSING TABLE PRSCBR_DENORM


  #Load BNFT_PLAN_RXC_CAG_PLAN_OPTNS
  iwdfs.BNFT_PLAN_RCP_OPTNS <- SelectDF(POC_MBR_OPPTY.V_BNFT_PLAN_RXC_CAG_PLAN_OPTNS)
  iwdfs.BNFT_PLAN_RCP_OPTNS <- aliasDF(iwdfs.BNFT_PLAN_RCP_OPTNS, "BNFT_PLAN_RCP_OPTNS", TRUE)
  #filter the STUS_CD for A
  iwdfs.BNFT_PLAN_RCP_OPTNS <- filter(
  iwdfs.BNFT_PLAN_RCP_OPTNS, iwdfs.BNFT_PLAN_RCP_OPTNS$BNFT_PLAN_RCP_OPTNS_STUS_CD == 'A')
  #join to table on LVL1_ACCT_GID AND LVL3_ACCT_GID
  targetTable <- join(targetTable, iwdfs.BNFT_PLAN_RCP_OPTNS,
    targetTable$PHMCY_LVL1_ACCT_GID == iwdfs.BNFT_PLAN_RCP_OPTNS$BNFT_PLAN_RCP_OPTNS_LVL1_ACCT_GID
    & targetTable$PHMCY_LVL3_ACCT_GID == iwdfs.BNFT_PLAN_RCP_OPTNS$BNFT_PLAN_RCP_OPTNS_LVL3_ACCT_GID)


  #To join V_MBR_PGM_RX_SCHD_HIST
  #A) First left outer join V_MBR_ACCT_HRCHY on MBR_ACCT_GID
  #B) Join left outer V_MBR_PGM_RX_SCHD_HIST ON B.MBR_ACCT_ID=C.QL_BNFCY_ID

  #A)
  #Load V_MBR_ACCT_HRCHY
  iwdfs.MBR_ACCT_HRCHY <- SelectDF(POC_MBR_OPPTY.V_MBR_ACCT_HRCHY)
  iwdfs.MBR_ACCT_HRCHY <- aliasDF(iwdfs.MBR_ACCT_HRCHY, "MBR_ACCT_HRCHY", TRUE)
  #filter the SRC_CD for Q
  iwdfs.MBR_ACCT_HRCHY <- filter(iwdfs.MBR_ACCT_HRCHY, iwdfs.MBR_ACCT_HRCHY$MBR_ACCT_HRCHY_SRC_CD == 'Q')
  #join to table on MBR_ACCT_GID
  #TODO Check if left outer
  targetTable <- join(targetTable, iwdfs.MBR_ACCT_HRCHY,
    targetTable$MBR_ACC_MBR_ACCT_GID == iwdfs.MBR_ACCT_HRCHY$MBR_ACCT_HRCHY_MBR_ACCT_GID,
    'left_outer')


  #B)
  #Load V_MBR_PGM_RX_SCHD_HIST
  iwdfs.MBR_PGM_RX_SCHD_HIST <- SelectDF(POC_MBR_OPPTY.V_MBR_PGM_RX_SCHD_HIST)
  iwdfs.MBR_PGM_RX_SCHD_HIST <- aliasDF(iwdfs.MBR_PGM_RX_SCHD_HIST, "MBR_PGM_RX_SCHD_HIST", TRUE)
  #Join V_MBR_PGM_RX_SCHD_HIST ON B.MBR_ACCT_ID=C.QL_BNFCY_ID  / SCHD_ENRL_BNFCY_ID
  #TODO Check if left outer
  targetTable <- join(targetTable, iwdfs.MBR_PGM_RX_SCHD_HIST,
    targetTable$MBR_ACCT_HRCHY_MBR_ACCT_GID == iwdfs.MBR_PGM_RX_SCHD_HIST$MBR_PGM_RX_SCHD_HIST_SCHD_ENRL_BNFCY_ID,
    'left_outer')

  return (targetTable)
}
