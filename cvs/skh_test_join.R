# Transformations

ffmInd <- function(latestClaimData) {
# To mark a claim as first fill mail by checking V_PHMCY_CLM.DLVRY_SYSTM_CD is equal to 'M' in the 
# last 6 months excluding the latest records .  
  # select min(fill_dt) for each EPH_LINK_ID
  
  # TODO Remove this join as we would get latest claim data full
  mbrAcctHist = SelectDF(skhmodata.V_MBR_ACCT_HIST)
  joinedmbr = join(latestClaimData, mbrAcctHist, latestClaimData$MBR_ACCT_GID == mbrAcctHist$MBR_ACCT_GID, "inner")
  aggregatedOldestClaim = agg(groupBy(joinedmbr, "eph_link_id", "GPI4_CD"), "fill_dt" -> "min")
  
  # join with latestclaim to get records for min fill_dt
  join(aggregatedOldestClaim, latestClaimData, )
  
  selectExpr(latestClaimData, "*", )
} 


########################################################################################################

stdDaySupply <- function(tableDf) { 
  
  dsqTableDf = selectExpr(tableDf, "*", "CASE WHEN LC_DAY_SPLY_QTY BETWEEN 1 AND 33  THEN 30
                            WHEN LC_DAY_SPLY_QTY BETWEEN 34 AND 83 THEN  60
                            WHEN LC_DAY_SPLY_QTY >=84  THEN 90
                            END as DAY_SPLY_QTY ")

  return <- dsqTableDf   
}

########################################################################################################

memberCurrentAge <- function(tablename) { 
  currentdate = Sys.Date()
  expr = paste("FLOOR(MONTHS_BETWEEN('", currentdate, "', ACCT_MBR_BRTH_DT) / 12) as MBR_CURR_AGE", sep = "")
  age = selectExpr(tablename, "*", expr )


  return <- age  
}

########################################################################################################

minorIndicator <- function(mydf) { 
  # case when MBR_CURR_AGE < 18 then 'Y' else 'N
  minorind = selectExpr(mydf, "*", "case when MBR_CURR_AGE < 18 then 'Y' else 'N' end as MINOR_IND")  
  return <-minorind
}

########################################################################################################

mbrEligIndicator <- function(mbrdf) { 
  # case when date between CVRG_EFF_DT and CVRG_EXPRN_DT then 'Y' else 'N'
  currentdate = Sys.Date()
  elig_expr = paste("case when'" ,  
      currentdate ,  "'between MAC_CVRG_EFF_DT and MAC_CVRG_EXPRN_DT then 'Y' else 'N' end as MBR_CURR_ELIG_IND",
      sep = "")
  mbrelig = selectExpr(mbrdf, "*", elig_expr)  
  return <-mbrelig  
}


########################################################################################################
# When corresponding client value has a MAIL_IND = 'Y' 
# and most recent claim record DLVRY_SYSTM_CD = 'R' then 'Y' else 'N' 

retailToMailIndicator <- function(mbrdf) {
  rtm_expr = selectExpr(mbrdf, "*", "case when BPRCPO_MAIL_IND = 'Y' then 'Y' else 'N' end  as RTM_IND") # TODO add for DLVRY_SYSTM_CD from phmcy claim
  return <- rtm_expr 
}

########################################################################################################
# when corresponding client value has a MAINT_CHOICE_TYP_CD='MV' and (retail 30d claim or nCVS 90d claim) then 'Y' else 'N'
# Meaning
# MCV_IND is set to ‘Y’ when  (MAINT_CHOICE_TYP_CD='MV' and DAY_SPLY_QTY BETWEEN 21 AND 33  AND DLVRY_SYSTM_CD =’R’)
# OR  (MAINT_CHOICE_TYP_CD='MV' and DAY_SPLY_QTY >=84  AND CVS_RTL_IND = ‘N’ AND CVS_MAIL_IND=’N’ )

maintenanceChoiceVoluntryIndicator <- function(mbrdf) {
  maintenanceChoiceVoluntryIndicatorDF = selectExpr(mbrdf, "*", "case when 
                              (BPRCPO_MAINT_CHOICE_TYP_CD='MV' AND DAY_SPLY_QTY BETWEEN 21 AND 33 AND LC_DLVRY_SYSTM_CD = 'R' ) 
                              OR
                              (BPRCPO_MAINT_CHOICE_TYP_CD ='MV' and DAY_SPLY_QTY >=84  AND PHMCY_DENORM_CVS_RTL_IND = 'N' AND PHMCY_DENORM_CVS_MAIL_IND='N' )
                              then 'Y' else 'N' end  as MCV_IND") 
  return <- maintenanceChoiceVoluntryIndicatorDF 
}
########################################################################################################
"When the MBR_ACCT_GIDs has a SCH_PGM_ID in (5383,5384) and SCHD_OPT_IN_PRCS_SRC_CD in (‘PTL’,’CPT’,’PSV’,’CPV’) and SCHD_OPT_OUT_DT of NULL or open ended.

5383=ACT AUTO REFILL PROGRAM   
5384=ACT AUTO RENEWAL PROGRAM 

ACT=Automatic Continuation of Therapy aka RFM
PTL = Portal 
CPT = Copy from Portal
PSV = Enrolled by phone
CPV = Copy from enrolled by phone.
"
readyFillAtMailIndicator < function(tableDf) {
  
  return <- tableDf ;  
}

########################################################################################################

# case when drg.DEA_CLS_CD in ('0',' ') then 'N' else 'Y'

controlledSubstanceInd <- function(tableDf) { 

  }


########################################################################################################
saveToHdfs <- function(hdfspath, transformed_df) {
  
}

########################################################################################################

source('~/demo/init.R')
source('~/demo/spark_api.R')
source('~/demo/mongo_api.R')
source('~/demo/myhelper.R')
source('~/demo/iw_tools.R')
source('~/demo/cvs_mo_transformations.R')

print(head(completeTable))

stdDaySupplyDF = stdDaySupply(completeTable)

age_added_view = memberCurrentAge(stdDaySupplyDF)
#print(head(age_added_view))

minorInd_added_view = minorIndicator(age_added_view)
#print(head(minorInd_added_view))

mbr_elig_view = mbrEligIndicator(minorInd_added_view)
#print(head(mbr_elig_view))

rtm_ind_view = retailToMailIndicator(mbr_elig_view)
#print(head(rtm_ind_view))

maintenanceChoiceVoluntryIndicatorDF = maintenanceChoiceVoluntryIndicator(rtm_ind_view)
print(head(rtm_ind_view)) 

















"
########################################################################################################

memberCoverageDataExtract <- function(latestclaim) {
# TODO Remove this join as we would get latest claim data full
mbrAcct = SelectDF(POC_MBR_OPPTY.V_MBR_ACCT_HIST)
mbrAcct = aliasDF(mbrAcct, "ACCT", TRUE)
latestclaim = aliasDF(latestclaim, "LC", TRUE)
joinedmbr = join(latestclaim , mbrAcct , latestclaim$LC_MBR_ACCT_GID == mbrAcct$ACCT_MBR_ACCT_GID,  inner)

# Get MBR_ACCT_CVRG
mbrAcctCoverage = SelectDF(POC_MBR_OPPTY.V_MBR_ACCT_CVRG)
mbrAcctCoverage = aliasDF(mbrAcctCoverage, "MAC", TRUE)

latestWithAcctCvg = join(joinedmbr, mbrAcctCoverage, 
joinedmbr$LC_MBR_ACCT_GID == mbrAcctCoverage$MAC_MBR_ACCT_GID, "inner")

# Get MBR_ACCT_CONT_CVRG 
mbrAcctCoverageCnt = SelectDF(POC_MBR_OPPTY.V_MBR_ACCT_CONT_CVRG)
mbrAcctCoverageCnt = aliasDF(mbrAcctCoverageCnt, "MAC_CNT", TRUE)

latestWithAcctCvgContd = 
join(latestWithAcctCvg, mbrAcctCoverageCnt, 
latestWithAcctCvg$LC_MBR_ACCT_GID == mbrAcctCoverageCnt$MAC_CNT_MBR_ACCT_GID, "leftouter")

return <- latestWithAcctCvgContd 
}

########################################################################################################
# 1.LVL1_ACCT_GID and LVL3_ACCT_GID in CLNT_ACCT_DENORM table must 
#  match the LVL1_ACCT_GID and LVL3_ACCT_GID in step 3 volatile table 
# .CURR_IND must be equal to  'Y' .

clientDataExtract <- function(latestWithAcctCvgContd) {
  clntAcctDnorm = SelectDF(POC_MBR_OPPTY.V_CLNT_ACCT_DENORM)
  clntAcctDnorm = aliasDF(clntAcctDnorm, "CAD", TRUE)
  
  clientDataExtract = join(latestWithAcctCvgContd, clntAcctDnorm, 
                           ( 
                             (latestWithAcctCvgContd$LC_LVL1_ACCT_GID == clntAcctDnorm$CAD_LVL1_ACCT_GID) & 
                               latestWithAcctCvgContd$LC_LVL3_ACCT_GID  == clntAcctDnorm$CAD_LVL3_ACCT_GID     
                           ), 
                           "inner")
  
  return <- clientDataExtract 
}

########################################################################################################

# 1.PHMCY_PTY_GID in PHMCY_DENORM table must be equal to PHMCY_PTY_GID in step 3 volatile table 
# 2.STUS_CD must be equal to  'A'

phmcyDataExtract <- function(latestclaim) {
  phmcyDemorm = SelectDF(POC_MBR_OPPTY.V_PHMCY_DENORM)
  phmcyDemorm = aliasDF(phmcyDemorm, "PHMCY_DENORM", TRUE)
  #print(latestclaim)
  
  phmcyDataExtracted = join(latestclaim, phmcyDemorm, ((latestclaim$LC_PHMCY_PTY_GID == phmcyDemorm$PHMCY_DENORM_PHMCY_PTY_GID) & (latestclaim$MAC_STUS_CD == 'A'))) 
  
  return <- phmcyDataExtracted
  
} 

########################################################################################################

createCompleteMO <- function(volDf) { 
  # Read these 3 more tables based upon the join conditions
  # V_PRSCBR_DENORM - TODO 
  # V_MBR_PGM_RX_SCHD_HIST
  # V_BNFT_PLAN_RXC_CAG_PLAN_OPTNS
  # VERIFY After knowing join condition 
  mbrPgxShdHist = SelectDF(POC_MBR_OPPTY.V_MBR_PGM_RX_SCHD_HIST)
  mbrAcctHrchy  = SelectDF(POC_MBR_OPPTY.V_MBR_ACCT_HRCHY)
  
  mbrPgxShdHist = aliasDF(mbrPgxShdHist, "MBRSH", TRUE)
  mbrAcctHrchy = aliasDF(mbrAcctHrchy, "MBRAH", TRUE)
  # join V_MBR_PGM_RX_SCHD_HIST and V_MBR_ACCT_HRCHY using MBR_ACCT_ID , SCHD_ENRL_BNFCY_ID
  mbrHistHrchy = join(mbrPgxShdHist, mbrAcctHrchy, mbrPgxShdHist$MBRSH_SCHD_ENRL_BNFCY_ID == mbrAcctHrchy$MBRAH_MBR_ACCT_ID)
  # join now with voldDf on MBR_ACCT_ID
  mbrHistHrchyClm = join(volDf, mbrHistHrchy, volDf$LC_MBR_ACCT_GID == mbrHistHrchy$MBRAH_MBR_ACCT_ID)
  
  
  cagPlanOptns = SelectDF(POC_MBR_OPPTY.V_BNFT_PLAN_RXC_CAG_PLAN_OPTNS)
  cagPlanOptns = aliasDF(cagPlanOptns, "BPRCPO", TRUE)
  
  finalJoin = join(mbrHistHrchyClm, cagPlanOptns , (volDf$LC_LVL1_ACCT_GID == cagPlanOptns$BPRCPO_LVL1_ACCT_GID) & 
                     (cagPlanOptns$BPRCPO_LVL3_ACCT_GID == mbrHistHrchyClm$LC_LVL3_ACCT_GID)) 
  
  return <- finalJoin 
}

#LoadSources(iw.sources.POC_MBR_OPPTY)
LoadSources(iw.sources.POC_MBR_OPPTY)

phmcyClaim  = SelectDF(POC_MBR_OPPTY.V_PHMCY_CLM, TRUE)
mcov = memberCoverageDataExtract(phmcyClaim)

clientextracted = clientDataExtract(mcov)

phmcyDataExtracted = phmcyDataExtract(clientextracted)

completeTable  = createCompleteMO(phmcyDataExtracted)

"
