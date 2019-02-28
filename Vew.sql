CREATE OR REPLACE VIEW MAIN_ORI_AERO_ADVICE_TABLE AS
with mds as(
SELECT
    id
  , datee
  , type_vs_code
  , incident_type
  , dubp_code
  , cnt_by_type
  , klass_sob
  , advice_dt_cur
  , advice_dt_prev
  , advice_dt_cnt_prev
  , case
    when LAST_ADV_DAY_CNT is null and advice_dt_prev is not null then (datee-advice_dt_prev)
    when advice_dt_prev is null then null
    else LAST_ADV_DAY_CNT
    end LAST_ADV_DAY_CNT
  FROM (
  SELECT id
      , datee
      , type_vs_code
      , incident_type
      , dubp_code
      , cnt_by_type
      , klass_sob
      , advice_dt advice_dt_cur
      , nvl(case when advice_dt_prev >= datee
                 then null else advice_dt_prev end
           ,lag(case when advice_dt_prev >= datee
                     then null else advice_dt_prev end ignore nulls) over(partition by dubp_code order by datee)
           ) advice_dt_prev
      , nvl(case when advice_dt_prev >= datee
                 then null else advice_dt_cnt_prev end
           ,lag(case when advice_dt_prev >= datee
                     then null else advice_dt_cnt_prev end ignore nulls) over(partition by dubp_code order by datee)
           ) advice_dt_cnt_prev
      , LAST_ADV_DT_CNT LAST_ADV_DAY_CNT
      FROM (
        SELECT
           id
          ,datee
          ,type_vs_code
          ,incident_type
          ,dubp_code
          ,cnt_by_type
          ,klass_sob
          ,advice_dt
          ,advice_dt_cnt
          ,lag(advice_dt) over(partition by dubp_code order by datee) advice_dt_prev
          ,lag(advice_dt_cnt) over(partition by dubp_code order by datee) advice_dt_cnt_prev
          ,case when lag(advice_dt) over(partition by dubp_code order by datee) < advice_dt then advice_dt-lag(advice_dt) over(partition by dubp_code order by datee) else null end LAST_ADV_DT_CNT
          FROM (
            SELECT
                moae.id
               ,moae.datee
               ,moae.type_vs_code
               ,moae.incident_type
               ,moae.dubp_code
               ,moae.cnt_by_type
               ,moae.klass_sob
               ,max(nvl(da.advice_finish, da.advice_deadline)) advice_dt
               ,count(nvl(da.advice_finish, da.advice_deadline)) advice_dt_cnt
            FROM (
              SELECT
                 ID
                ,DATEE
                ,TYPE_VS_CODE
                ,INCIDENT_TYPE
                ,DUBP_CODE
                ,cnt_by_type
                ,KLASS_SOB
                FROM (
                  select
                      e.DUBP_ID ID
                     ,e.dubp_date DATEE
                     ,e.TYPE_FAMILY TYPE_VS_CODE
                     ,e.incident_type INCIDENT_TYPE
                     ,/*NVL(GRP.DUBP_GROUP, e.dubp_code)*/e.dubp_code DUBP_CODE
                     ,e.cnt_by_type
                     ,e.klass_sob KLASS_SOB
                       FROM adubp_risk_events e
                      --  JOIN GRP ON GRP.DUBP_CODE = e.DUBP_CODE
                )
                     WHERE TYPE_VS_CODE is not null
            ) moae
            left join Dubp_Advices da on moae.id = da.advice_object_id and da.advice_efficiency_id = -1813 and da.advice_status_id = 10005386 and da.is_active = 1
            group by moae.id
                 ,moae.datee
                 ,moae.type_vs_code
                 ,moae.incident_type
                 ,moae.dubp_code
                 ,moae.cnt_by_type
                 ,moae.klass_sob
         )
   )
)
)
, grp as(
select ds.subset_name DUBP_GROUP, dv.value DUBP_CODE
  from directory_subsets ds, directory_subset_values dsv, directory_values dv
    where ds.subset_directory_id=-1710
      and ds.id in(-1711,-1712,-1713,-1714,-1715,-1716,-1717,-1718,-1719,-1720)
      and ds.id = dsv.value_subset_id
      and dsv.value_id = dv.id
)
select
    mds.id
  , mds.datee
  , mds.type_vs_code
  , ORA_HASH(mds.type_vs_code,10000,0) type_vs_code_ID
  , mds.incident_type
  , ORA_HASH(substr(mds.incident_type,1,instr(mds.incident_type,' ')-1),10000,0) incident_t_short_ID
  , mds.dubp_code
  , substr(mds.DUBP_CODE,1,instr(mds.DUBP_CODE,' ')-1) DUBP_CODE_NUM
  , ORA_HASH(substr(mds.DUBP_CODE,1,instr(mds.DUBP_CODE,' ')-1),10000,0) DUBP_CODE_ID
  , listagg(grp.DUBP_GROUP,';') WITHIN GROUP (ORDER BY grp.DUBP_GROUP) CODE_GROUP
  , mds.cnt_by_type
  , mds.klass_sob
  , mds.advice_dt_cur
  , mds.advice_dt_prev
  , mds.advice_dt_cnt_prev
  , mds.LAST_ADV_DAY_CNT
FROM mds
 left join grp on grp.DUBP_CODE = mds.DUBP_CODE
 WHERE (mds.advice_dt_cur <= sysdate OR mds.advice_dt_cur IS NULL)
   AND (mds.advice_dt_prev <= sysdate OR mds.advice_dt_prev IS NULL)
GROUP BY     mds.id
  , mds.datee
  , mds.type_vs_code
  , mds.incident_type
  , mds.dubp_code
  , mds.cnt_by_type
  , mds.klass_sob
  , mds.advice_dt_cur
  , mds.advice_dt_prev
  , mds.advice_dt_cnt_prev
  , mds.LAST_ADV_DAY_CNT
order by mds.datee;
