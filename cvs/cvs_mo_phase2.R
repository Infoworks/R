# Transformations
source('cvs_mo_phase1.R')


ffmInd <- function(latestClaimData) {
# To mark a claim as first fill mail by checking V_PHMCY_CLM.DLVRY_SYSTM_CD is equal to 'M' in the 
# last 6 months excluding the latest records .  
  # select min(fill_dt) for each EPH_LINK_ID
  
  # TODO Remove this join as we would get latest claim data full
  mbrAcctHist = SelectDF(POC_MBR_OPPTY.V_MBR_ACCT_HIST)
  joinedmbr = join(latestClaimData, mbrAcctHist, latestClaimData$MBR_ACCT_GID == mbrAcctHist$MBR_ACCT_GID, "inner")
  aggregatedOldestClaim = agg(groupBy(joinedmbr, "eph_link_id", "GPI4_CD"), "fill_dt" -> "min")
  
  # join with latestclaim to get records for min fill_dt
  join(aggregatedOldestClaim, latestClaimData,  "")
  
  selectExpr(latestClaimData, "*", "")
} 


########################################################################################################

stdDaySupply <- function(tableDf) { 
  
  #print(head(selectExpr(tableDf, "PHMCY_DAY_SPLY_QTY")))
  dsqTableDf = selectExpr(tableDf, "*", "CASE WHEN PHMCY_DAY_SPLY_QTY BETWEEN 1 AND 33  THEN 30
                            WHEN PHMCY_DAY_SPLY_QTY BETWEEN 34 AND 83 THEN  60
                            WHEN PHMCY_DAY_SPLY_QTY >=84  THEN 90
                            END as NORMALIZED_DAY_SPLY_QTY ")

  #print(head(selectExpr(dsqTableDf, "PHMCY_DAY_SPLY_QTY", "NORMALIZED_DAY_SPLY_QTY")))
  return <- dsqTableDf   
}

########################################################################################################

memberCurrentAge <- function(tablename) { 
  currentdate = Sys.Date()
  #print(head(selectExpr(tablename, "MBR_ACC_MBR_BRTH_DT")))
  expr = paste("FLOOR(MONTHS_BETWEEN('", currentdate, "', MBR_ACC_MBR_BRTH_DT) / 12) as MBR_CURR_AGE", sep = "")
  ageDF = selectExpr(tablename, "*", expr )

  #print(head(select(ageDF, "MBR_ACC_MBR_BRTH_DT", "MBR_CURR_AGE")))
  return <- ageDF  
}

########################################################################################################

minorIndicator <- function(mydf) { 
  # case when MBR_CURR_AGE < 18 then 'Y' else 'N
  minorIndDF = selectExpr(mydf, "*", "case when MBR_CURR_AGE < 18 then 'Y' else 'N' end  as MINOR_IND")  
  #print(head(select(minorIndDF,  "MBR_CURR_AGE", "MINOR_IND")))
  return <-minorIndDF
}

########################################################################################################

mbrEligIndicator <- function(mbrdf) { 
  # case when date between CVRG_EFF_DT and CVRG_EXPRN_DT then 'Y' else 'N'
  #print(head(selectExpr(mbrdf, "MBR_ACC_CVRG_CVRG_EFF_DT", "MBR_ACC_CVRG_CVRG_EXPRN_DT")))
  
  currentdate = Sys.Date()
  elig_expr = paste("case when'" ,  
      currentdate ,  "'between MBR_ACC_CVRG_CVRG_EFF_DT and MBR_ACC_CVRG_CVRG_EXPRN_DT 
        then 'Y' else 'N' end as MBR_CURR_ELIG_IND",
      sep = "")
  mbrelig = selectExpr(mbrdf, "*", elig_expr)  
  
  #print(head(select(mbrelig, "MBR_ACC_CVRG_CVRG_EFF_DT", "MBR_ACC_CVRG_CVRG_EXPRN_DT", "MBR_CURR_ELIG_IND")))
  return <-mbrelig  
}


########################################################################################################
# When corresponding client value has a MAIL_IND = 'Y' 
# and most recent claim record DLVRY_SYSTM_CD = 'R' then 'Y' else 'N' 

retailToMailIndicator <- function(mbrdf) {
  rtm_exprDf = selectExpr(mbrdf, "*", "case when BNFT_PLAN_RCP_OPTNS_MAIL_IND = 'Y' then 'Y' else 'N' end  as RTM_IND") # TODO add for DLVRY_SYSTM_CD from phmcy claim
  
  #print(head(select(rtm_exprDf, "BNFT_PLAN_RCP_OPTNS_MAIL_IND", "RTM_IND")))
  return <- rtm_exprDf 
}

########################################################################################################
# when corresponding client value has a MAINT_CHOICE_TYP_CD='MV' and (retail 30d claim or nCVS 90d claim) then 'Y' else 'N'
# Meaning
# MCV_IND is set to ‘Y’ when  (MAINT_CHOICE_TYP_CD='MV' and DAY_SPLY_QTY BETWEEN 21 AND 33  AND DLVRY_SYSTM_CD =’R’)
# OR  (MAINT_CHOICE_TYP_CD='MV' and DAY_SPLY_QTY >=84  AND CVS_RTL_IND = ‘N’ AND CVS_MAIL_IND=’N’ )

maintenanceChoiceVoluntryIndicator <- function(mbrdf) {
  maintenanceChoiceVoluntryIndicatorDF = selectExpr(mbrdf, "*", "case when 
                              (BNFT_PLAN_RCP_OPTNS_MAINT_CHOICE_TYP_CD='MV' AND 
                              PHMCY_DAY_SPLY_QTY BETWEEN 21 AND 33 AND PHMCY_DLVRY_SYSTM_CD = 'R' ) 
                              OR
                              (BNFT_PLAN_RCP_OPTNS_MAINT_CHOICE_TYP_CD ='MV' 
                              and PHMCY_DAY_SPLY_QTY >=84  AND 
                              PHMCY_DENORM_CVS_RTL_IND = 'N' AND PHMCY_DENORM_CVS_MAIL_IND='N' )
                              then 'Y' else 'N' end  as MCV_IND")
  
  #print(head(select(maintenanceChoiceVoluntryIndicatorDF, "PHMCY_CLM_EVNT_GID", "BNFT_PLAN_RCP_OPTNS_MAINT_CHOICE_TYP_CD", "PHMCY_DAY_SPLY_QTY" , "PHMCY_DLVRY_SYSTM_CD", "PHMCY_DENORM_CVS_RTL_IND", "PHMCY_DENORM_CVS_MAIL_IND", "MCV_IND" )))
  return <- maintenanceChoiceVoluntryIndicatorDF 
}
########################################################################################################
#When the MBR_ACCT_GIDs has a SCH_PGM_ID in (5383,5384) and SCHD_OPT_IN_PRCS_SRC_CD in (‘PTL’,’CPT’,’PSV’,’CPV’) and SCHD_OPT_OUT_DT of NULL or open ended.

#5383=ACT AUTO REFILL PROGRAM   
#5384=ACT AUTO RENEWAL PROGRAM 

#ACT=Automatic Continuation of Therapy aka RFM
#PTL = Portal 
#CPT = Copy from Portal
#PSV = Enrolled by phone
#CPV = Copy from enrolled by phone.

#readyFillAtMailIndicator < function(tableDf) {
  
 # return <- tableDf ;  
#}

########################################################################################################

  # case when drg.DEA_CLS_CD in ('0',' ') then 'N' else 'Y'

controlledSubstanceInd <- function(tableDf) { 
  controlledSubstanceIndDf <- selectExpr(tableDf, "*", "case when DRUG_DEA_CLS_CD in ('0',' ') then 'N' else 'Y' end as CONTROLLED_SUBSTANCE_IND ")
  #print(head(select(controlledSubstanceIndDf, "DRUG_DEA_CLS_CD" , "CONTROLLED_SUBSTANCE_IND")))
  return <- controlledSubstanceIndDf 

}

########################################################################################################
cmpgnMbrOpptyTable <- function(joineddf) { 
  
  return <- selectExpr(joineddf, "123 as MBR_OPPTY_GID", "MBR_ACC_EPH_LINK_ID as EPH_LINK_ID", "DRUG_GPI4_CD as GPI4_CD", " PHMCY_LVL1_ACCT_GID as LVL1_ACCT_GID", 
             " PHMCY_LVL3_ACCT_GID as LVL3_ACCT_GID",  " CLNT_ACCT_DENORM_LVL0_ACCT_ID as LVL0_ACCT_ID", " CLNT_ACCT_DENORM_LVL0_ACCT_NM as LVL0_ACCT_NM", 
             " CLNT_ACCT_DENORM_LVL0_EFF_DT as LVL0_EFF_DT", " CLNT_ACCT_DENORM_LVL0_EXPRN_DT as LVL0_EXPRN_DT", " CLNT_ACCT_DENORM_LVL1_ACCT_ID as LVL1_ACCT_ID", 
             " CLNT_ACCT_DENORM_LVL1_ACCT_NM as LVL1_ACCT_NM", " CLNT_ACCT_DENORM_LVL1_EFF_DT as LVL1_EFF_DT", " CLNT_ACCT_DENORM_LVL1_EXPRN_DT as LVL1_EXPRN_DT", 
             " CLNT_ACCT_DENORM_NON_PBM_CLNT_TYP_CD as NON_PBM_CLNT_TYP_CD", " CLNT_ACCT_DENORM_NON_PBM_LOB_CD as NON_PBM_LOB_CD", 
             " DRUG_DRUG_PROD_GID as DRUG_PROD_GID", " PHMCY_CLM_EVNT_GID as LAST_FILL_CLM_GID", 
             " PHMCY_FILL_DT as LAST_FILL_DT", " PHMCY_MBR_ACCT_GID as MBR_ACCT_GID", " MBR_ACC_CVRG_CVRG_EFF_DT as CVRG_EFF_DT", 
             " MBR_ACC_CVRG_CVRG_EXPRN_DT as CVRG_EXPRN_DT", " MBR_ACC_CONT_CVRG_ELIG_CC_EFF_DT as ELIG_CC_EFF_DT", 
             " MBR_ACC_CONT_CVRG_ELIG_CC_EXPRN_DT as ELIG_CC_EXPRN_DT", " PHMCY_DENORM_PHMCY_PTY_GID as PHMCY_PTY_GID", " PHMCY_DENORM_CVS_MAIL_IND as CVS_MAIL_IND", 
             " PHMCY_SRC_CD as CLM_SRC_CD", " PHMCY_PRSCBR_PTY_GID as PRSCBR_PTY_GID", " PHMCY_DLVRY_SYSTM_CD as DLVRY_SYSTM_CD", 
             " PHMCY_DAY_SPLY_QTY as DAY_SPLY_QTY", "NORMALIZED_DAY_SPLY_QTY as NORMALIZED_DAY_SPLY_QTY", " PHMCY_ADJD_FRMLY_CD as ADJD_FRMLY_CD", 
             " PHMCY_MEDD_CLM_IND as MEDD_CLM_IND", " MBR_ACC_GNDR_CD as GNDR_CD",  " MBR_CURR_AGE as MBR_CURR_AGE", 
             " MINOR_IND as MINOR_IND", " MBR_CURR_ELIG_IND as MBR_CURR_ELIG_IND", " RTM_IND as RTM_IND", "MCV_IND as MCV_IND", 
             # TODO " as RFM_IND", 
             # TODO " as FFM_IND", 
             " CONTROLLED_SUBSTANCE_IND as CONTROLLED_SUBSTANCE_IND", " DRUG_BUS_MAINT_IND as MAINTENANCE_IND", 
             " PHMCY_PHMCY_LTC_IND as PHMCY_LTC_IND", " DRUG_SPCLT_DRUG_IND as SPCLT_DRUG_IND", paste("'" ,Sys.time() , "'" ," as CREATE_TMS", sep = ""))
  
}

########################################################################################################
saveToHdfs <- function(df, host, path) {
  writeToHdfs(df, host,path)
  print("done !!!")
}

########################################################################################################


prepareMoCampaignTable <- function() {

  LoadSources(iw.sources.POC_MBR_OPPTY)
  print("Phase 1 join starting")

  iwdf = "" 
  completeTable = prepare_p2_table(iw.sources.POC_MBR_OPPTY)
  #print(head(select(completeTable, "PHMCY_CLM_EVNT_GID", "PHMCY_DAY_SPLY_QTY", "MBR_ACC_MBR_BRTH_DT", "MBR_ACC_CVRG_CVRG_EFF_DT", "MBR_ACC_CVRG_CVRG_EXPRN_DT", "BNFT_PLAN_RCP_OPTNS_MAIL_IND", "BNFT_PLAN_RCP_OPTNS_MAINT_CHOICE_TYP_CD", "PHMCY_DENORM_CVS_RTL_IND", "PHMCY_DENORM_CVS_MAIL_IND")))

  stdDaySupplyDF = stdDaySupply(completeTable)
  #print(head(stdDaySupplyDF))

  age_added_view = memberCurrentAge(stdDaySupplyDF)
  #print(head(age_added_view))

  minorInd_added_view = minorIndicator(age_added_view)
  #print(head(minorInd_added_view))

  mbr_elig_view = mbrEligIndicator(minorInd_added_view)
  #print(head(mbr_elig_view))

  rtm_ind_view = retailToMailIndicator(mbr_elig_view)
  #print(head(rtm_ind_view))

  maintenanceChoiceVoluntryIndicatorDF = maintenanceChoiceVoluntryIndicator(rtm_ind_view)
  #print(head(maintenanceChoiceVoluntryIndicatorDF)) 

  controlledSubstanceIndDf = controlledSubstanceInd(maintenanceChoiceVoluntryIndicatorDF)
  #print(head(maintenanceChoiceVoluntryIndicatorDF)) 

  cmpgnMbrOpptyTableDf = cmpgnMbrOpptyTable(controlledSubstanceIndDf)

  return (cmpgnMbrOpptyTableDf)

}