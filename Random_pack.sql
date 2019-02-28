create or replace package pma_data_set_utils is

  -- Author  : Alexander Vasiliev
  -- Created : 31.08.2018
  -- Purpose : Load data to pma_data_set table   
   
   PROCEDURE run_this(p_model_id INTEGER);
   PROCEDURE load_pma_data_set(p_model_id INTEGER);
   PROCEDURE load_first_advice_day_cnt_2(p_model_id INTEGER);
   PROCEDURE load_advice_day_cnt0_2(p_model_id INTEGER);
   PROCEDURE updt_adv_dist_category(p_model_id INTEGER);

   FUNCTION get_dubp_code(p_model_id INTEGER, p_short_dubp_code_id IN INTEGER) RETURN VARCHAR2;
   FUNCTION get_short_dubp_code_id(p_model_id INTEGER, p_dubp_code IN VARCHAR2) RETURN INTEGER;
   PROCEDURE load_pma_result_3 (p_model_id INTEGER);
   PROCEDURE load_final_k (p_model_id INTEGER);
   PROCEDURE run_all;
   FUNCTION get_dubp_advice_text(p_date in DATE, p_id in NUMBER, p_group_name IN VARCHAR2) return  varchar2;

end pma_data_set_utils;
/
create or replace package body pma_data_set_utils is

   -- If we need to clear the calculation table of the influence of measures
   -- TRUNCATE TABLE pma_result3;
   -- TRUNCATE TABLE pma_code_and_k;
   -- TRUNCATE TABLE pma_code_and_k_final;
   -- TRUNCATE TABLE pma_prd_tuning;
   -- TRUNCATE TABLE pma_data_set;
   
   PROCEDURE run_this(p_model_id INTEGER) IS
      v_count INTEGER;
   BEGIN
      SELECT count(*) INTO v_count
         FROM pma_data_set ds WHERE ds.models_id = p_model_id;
      IF v_count = 0 THEN
         load_pma_data_set(p_model_id);
         load_pma_result_3(p_model_id);
         load_final_k(p_model_id);
      END IF;
   END;


   -- ==================================================================================
   -- ||                         this for Unique groups                               ||
   -- ==================================================================================
   PROCEDURE load_pma_data_set(p_model_id INTEGER) IS
      v_date_start DATE;
      v_date_end   DATE;
      v_dt_counts  INTEGER;
      v_cur_day    INTEGER;

      v_datee               DATE;
      v_dubp_code           VARCHAR2(1000);
      v_short_dubt_code_id  INTEGER;
      v_short_dubt_code_id2 INTEGER := -1;
      v_group_code_id       INTEGER;
      v_group_code_id2      INTEGER := -1;
      v_last_adv_day_cnt    INTEGER;
      v_last_adv_day_cnt2   INTEGER := -1;
      v_models_date_num     INTEGER;
      v_models_date_num2    INTEGER := -1;

      CURSOR cur00_dc_id IS SELECT DISTINCT mds.short_dubt_code_id, ds.id
         FROM directory_subsets ds, directory_subset_values dsv, directory_values dv, model_data_set mds
            WHERE ds.subset_directory_id=-1710
               AND ds.id in(-1711,-1712,-1713,-1714,-1715,-1716,-1717,-1718,-1719,-1720)
               AND ds.id = dsv.value_subset_id  AND dsv.value_id = dv.id
               AND dv.value IN mds.dubp_code AND mds.code_group IS NOT NULL
         ORDER BY 1, 2;

      CURSOR cur01_dc_id IS SELECT mds.datee, mds.dubp_code, mds.short_dubt_code_id, count(*)
         FROM model_data_set mds WHERE mds.models_id = p_model_id AND mds.code_group IS     NULL GROUP BY mds.datee, mds.dubp_code, mds.short_dubt_code_id;
      CURSOR cur02_dc_id IS SELECT mds.datee, mds.dubp_code, mds.short_dubt_code_id, count(*)
         FROM model_data_set mds WHERE mds.models_id = p_model_id AND mds.code_group IS NOT NULL GROUP BY mds.datee, mds.dubp_code, mds.short_dubt_code_id;

      CURSOR cur03_dubt_code_id IS SELECT pds.datee, pds.models_date_num, pds.short_dubt_code_id, pds.last_adv_day_cnt
        FROM pma_data_set pds  WHERE pds.models_id = p_model_id AND pds.short_dubt_code_id IS NOT NULL
        AND pds.last_adv_day_cnt IS NOT NULL  -- AND pds.last_adv_day_cnt = -888888
        AND pds.group_code_id IS NULL
        ORDER BY pds.short_dubt_code_id, pds.models_date_num;

      CURSOR cur04_group_code_id IS SELECT pds.datee, pds.group_code_id, pds.models_date_num, min(pds.last_adv_day_cnt)
        FROM pma_data_set pds  WHERE pds.models_id = p_model_id AND pds.group_code_id IS NOT NULL
        AND pds.last_adv_day_cnt IS NOT NULL  --AND pds.last_adv_day_cnt = -888888
        GROUP BY pds.datee, pds.models_date_num, pds.group_code_id
        ORDER BY pds.group_code_id, pds.models_date_num;


   BEGIN
      SELECT TO_DATE(pm.date_start, 'YYYY-MM-DD'), TO_DATE(pm.date_end, 'YYYY-MM-DD') INTO v_date_start, v_date_end
         FROM pawlin_models pm WHERE pm.id = p_model_id;
      v_dt_counts := v_date_end - v_date_start + 1;
      --  1) for ALL DAYS we add all the lines where code_group is NULL
      FOR v_cur_day IN 1..v_dt_counts
      LOOP
         INSERT INTO pma_data_set(models_id,
                                  datee,
                                  models_date_num,
                                  short_dubt_code_id,
                                  event_fact)
              SELECT DISTINCT  p_model_id,
                               v_date_start - 1 + v_cur_day,
                               v_cur_day,
                               md.short_dubt_code_id,
                               0
                    FROM model_data_set md WHERE md.models_id = p_model_id AND md.code_group IS NULL ORDER BY md.short_dubt_code_id;
      END LOOP;

     --  2) ... и где code_group IS NOT NULL

     OPEN cur00_dc_id;
      LOOP
         FETCH cur00_dc_id INTO v_short_dubt_code_id, v_group_code_id;
         EXIT WHEN cur00_dc_id%NOTFOUND;
            FOR v_cur_day IN 1..v_dt_counts
            LOOP
              INSERT INTO pma_data_set(models_id,
                                  datee,
                                  models_date_num,
                                  short_dubt_code_id,
                                  group_code_id,
                                  event_fact)
                     VALUES (p_model_id,
                               v_date_start - 1 + v_cur_day,
                               v_cur_day,
                               v_short_dubt_code_id,
                               v_group_code_id,
                               0);
            END LOOP;
      END LOOP;
      CLOSE cur00_dc_id;
      COMMIT;

      -- 3) Increase event counters (event_fact) for NOT grouped events.
      OPEN cur01_dc_id;
      LOOP
         FETCH cur01_dc_id INTO v_datee, v_dubp_code, v_short_dubt_code_id, v_dt_counts;
         EXIT WHEN cur01_dc_id%NOTFOUND;

         UPDATE pma_data_set pds
            SET   pds.event_fact = pds.event_fact + v_dt_counts
            WHERE pds.datee = v_datee AND pds.short_dubt_code_id = v_short_dubt_code_id AND pds.models_id = p_model_id;

      END LOOP;
      CLOSE cur01_dc_id;
      COMMIT;


      -- 4) Увеличим счетчики событий event_fact для СГРУППИРОВАННЫХ событий
      -- Now, in fact, the same logic as in paragraph 4
      OPEN cur02_dc_id;
      LOOP
         FETCH cur02_dc_id INTO v_datee, v_dubp_code, v_short_dubt_code_id, v_dt_counts;
         EXIT WHEN cur02_dc_id%NOTFOUND;
            UPDATE pma_data_set pds
               SET   pds.event_fact = pds.event_fact + v_dt_counts
               WHERE pds.datee = v_datee AND pds.short_dubt_code_id = v_short_dubt_code_id AND pds.models_id = p_model_id;
      END LOOP;
      CLOSE cur02_dc_id;
      COMMIT;


      -- Set the last_adv_day_cnt for the first lines:
      
      load_first_advice_day_cnt_2(p_model_id);

      -- Dot fill
      -- cnt0_2 - on view from Anatolia.
      load_advice_day_cnt0_2(p_model_id);


      -- 6) put the last_adv_day_cnt values in the remaining cells for single, NOT grouped events
      OPEN cur03_dubt_code_id;
      LOOP
         FETCH cur03_dubt_code_id INTO v_datee, v_models_date_num, v_short_dubt_code_id, v_last_adv_day_cnt;
         EXIT WHEN cur03_dubt_code_id%NOTFOUND;

         IF v_short_dubt_code_id <> v_short_dubt_code_id2 AND v_short_dubt_code_id2 <> -1 THEN
         -- Switched to a new code.
            UPDATE pma_data_set pds SET pds.last_adv_day_cnt = v_last_adv_day_cnt2 - v_models_date_num2 +  models_date_num
                  WHERE pds.short_dubt_code_id = v_short_dubt_code_id2 AND pds.models_id = p_model_id
                  AND v_last_adv_day_cnt2 - v_models_date_num2 +  models_date_num > 0
                  AND pds.models_date_num > v_models_date_num2;
             v_models_date_num2 := -1;
             v_short_dubt_code_id2 := -1;
             v_last_adv_day_cnt2 := -1;
         END IF;


            -- Update the data for the lines above if you missed.
         IF v_models_date_num2 = -1 THEN
            IF v_models_date_num > 1 THEN
               UPDATE pma_data_set pds SET pds.last_adv_day_cnt = v_last_adv_day_cnt - v_models_date_num +  models_date_num
                  WHERE pds.short_dubt_code_id = v_short_dubt_code_id AND pds.models_id = p_model_id
                  AND v_last_adv_day_cnt - v_models_date_num +  models_date_num > 0
                  AND pds.models_date_num < v_models_date_num;
            END IF;
               v_models_date_num2 := v_models_date_num;
               v_short_dubt_code_id2 := v_short_dubt_code_id;
               v_last_adv_day_cnt2 := v_last_adv_day_cnt;
           -- Update the data for the lines below (if any).
         ELSE
            -- top to bottom 0
            UPDATE pma_data_set pds SET pds.last_adv_day_cnt = v_last_adv_day_cnt2 - v_models_date_num2 +  models_date_num
               WHERE pds.short_dubt_code_id = v_short_dubt_code_id AND pds.models_id = p_model_id
               AND pds.models_date_num < v_models_date_num
               AND pds.models_date_num > v_models_date_num2
               AND v_last_adv_day_cnt2 - v_models_date_num2 +  models_date_num > 0;
            -- bottom to 0
            UPDATE pma_data_set pds SET pds.last_adv_day_cnt = v_last_adv_day_cnt - v_models_date_num +  models_date_num
               WHERE pds.short_dubt_code_id = v_short_dubt_code_id AND pds.models_id = p_model_id
               AND pds.models_date_num < v_models_date_num
               AND pds.models_date_num > v_models_date_num2
               AND v_last_adv_day_cnt - v_models_date_num +  models_date_num > 0;
            v_models_date_num2 := v_models_date_num;
            v_short_dubt_code_id2 := v_short_dubt_code_id;
            v_last_adv_day_cnt2 := v_last_adv_day_cnt;
         END IF;

      END LOOP;
      CLOSE cur03_dubt_code_id;

      IF v_short_dubt_code_id2 <> -1 AND v_models_date_num2 <> -1 THEN
         -- Update the latest code.
         UPDATE pma_data_set pds SET pds.last_adv_day_cnt = v_last_adv_day_cnt2 - v_models_date_num2 +  models_date_num
                  WHERE pds.short_dubt_code_id = v_short_dubt_code_id2 AND pds.models_id = p_model_id
                  AND pds.models_date_num > v_models_date_num2
                  AND v_last_adv_day_cnt2 - v_models_date_num2 +  models_date_num > 0;
      END IF;
      COMMIT;

      -- 7) put the last_adv_day_cnt values in the remaining cells for grouped events
      v_models_date_num2 := -1;
      v_group_code_id2 := -1;
      v_last_adv_day_cnt2 := -1;
      OPEN cur04_group_code_id;
      LOOP
         FETCH cur04_group_code_id INTO v_datee, v_group_code_id, v_models_date_num, v_last_adv_day_cnt;
         EXIT WHEN cur04_group_code_id%NOTFOUND;

         IF v_group_code_id <> v_group_code_id2  AND v_group_code_id2 <> -1 THEN
         -- Switched to a new code.
            UPDATE pma_data_set pds
              SET pds.last_adv_day_cnt = v_last_adv_day_cnt2 - v_models_date_num2 +  models_date_num
                  WHERE pds.group_code_id = v_group_code_id2 -- AND pds.short_dubt_code_id = v_short_dubt_code_id
                  AND pds.models_id = p_model_id
                  AND pds.models_date_num > v_models_date_num2
                  AND v_last_adv_day_cnt2 - v_models_date_num2 +  models_date_num > 0;
             v_models_date_num2 := -1;
             v_group_code_id2 := -1;
             v_last_adv_day_cnt2 := -1;
         END IF;

            -- Update the data for the lines above if you missed.
         IF v_models_date_num2 = -1 THEN
            IF v_models_date_num > 1 THEN
               UPDATE pma_data_set pds SET pds.last_adv_day_cnt = v_last_adv_day_cnt - v_models_date_num +  models_date_num
                  WHERE pds.group_code_id = v_group_code_id
                  AND pds.models_id = p_model_id
                  AND pds.models_date_num < v_models_date_num
                  AND v_last_adv_day_cnt - v_models_date_num +  models_date_num > 0;
            END IF;
               v_models_date_num2 := v_models_date_num;
               v_group_code_id2 := v_group_code_id;
               v_last_adv_day_cnt2 := v_last_adv_day_cnt;
           -- Update the data for the lines below (if any).
         ELSE
           -- If you stumbled upon an older event - we should not take it into account.
            IF v_last_adv_day_cnt > v_last_adv_day_cnt2 + v_models_date_num - v_models_date_num2 THEN
               v_last_adv_day_cnt := v_last_adv_day_cnt2 + v_models_date_num - v_models_date_num2;
            END IF;

            -- top to bottom 0
            UPDATE pma_data_set pds SET pds.last_adv_day_cnt = v_last_adv_day_cnt2 - v_models_date_num2 +  models_date_num
               WHERE pds.group_code_id = v_group_code_id
               AND pds.models_id = p_model_id
               AND pds.models_date_num < v_models_date_num
               AND pds.models_date_num > v_models_date_num2
               AND v_last_adv_day_cnt2 - v_models_date_num2 +  models_date_num > 0;
            -- bottom to 0
            UPDATE pma_data_set pds SET pds.last_adv_day_cnt = v_last_adv_day_cnt - v_models_date_num +  models_date_num
               WHERE pds.group_code_id = v_group_code_id
               AND pds.models_id = p_model_id
               AND pds.models_date_num < v_models_date_num
               AND pds.models_date_num > v_models_date_num2
               AND v_last_adv_day_cnt - v_models_date_num +  models_date_num > 0;
            v_models_date_num2 := v_models_date_num;
            v_group_code_id2 := v_group_code_id;
            v_last_adv_day_cnt2 := v_last_adv_day_cnt;
         END IF;
      END LOOP;
    COMMIT;

      CLOSE cur04_group_code_id;

      IF v_group_code_id2 <> -1 AND v_models_date_num2 <> -1 THEN
         -- Update the latest code.
         UPDATE pma_data_set pds SET pds.last_adv_day_cnt = v_last_adv_day_cnt2 - v_models_date_num2 +  models_date_num
            WHERE pds.group_code_id = v_group_code_id2
            AND pds.models_id = p_model_id
            AND pds.models_date_num > v_models_date_num2;
      END IF;
      COMMIT;

      -- write down short / medium / long
      updt_adv_dist_category(p_model_id);

   END load_pma_data_set;


   -- ====================================================================================
   -- ||      Fill in the last_adv_day_cnt from main_advice for the first lines 		||
   -- ====================================================================================
   PROCEDURE load_first_advice_day_cnt_2 (p_model_id INTEGER) IS
      max_adv_day_cnt      INTEGER;
      v_advice_date        DATE;
      v_short_dubt_code_id INTEGER;
      v_group_code_id      INTEGER;
      v_dubt_code          VARCHAR2(1000);
      v_date_start         DATE;
      v_date_end           DATE;
      v_moaa_table_id      INTEGER;

      CURSOR cur01_all_dubt_code1 IS SELECT DISTINCT  md.short_dubt_code_id, md.dubp_code
         FROM model_data_set md
         WHERE md.models_id = p_model_id --AND md.short_dubt_code_id = 1472
         AND md.code_group IS       NULL ORDER BY md.short_dubt_code_id;

      CURSOR cur02_all_group_code2 IS SELECT DISTINCT ds.group_code_id
         FROM pma_data_set ds WHERE ds.models_id = p_model_id
         AND ds.group_code_id IS NOT NULL ORDER BY ds.group_code_id;

   BEGIN
      SELECT TO_DATE(pm.date_start, 'YYYY-MM-DD'), TO_DATE(pm.date_end, 'YYYY-MM-DD') INTO v_date_start, v_date_end
         FROM pawlin_models pm WHERE pm.id = p_model_id;

      OPEN cur01_all_dubt_code1;
      LOOP
         FETCH cur01_all_dubt_code1 INTO v_short_dubt_code_id, v_dubt_code;
         EXIT WHEN cur01_all_dubt_code1%NOTFOUND;
             SELECT max(advice_dt_cur) INTO v_advice_date
                 FROM (
                     SELECT datee, advice_dt_cur
                     FROM main_ori_aero_advice_table mo
                     WHERE  mo.dubp_code = v_dubt_code
                     AND advice_dt_cur <= v_date_start
                 ) WHERE advice_dt_cur is not null;
         IF v_advice_date IS NULL THEN
            max_adv_day_cnt := 999000;
            v_moaa_table_id := -1;
         ELSE
            max_adv_day_cnt := v_date_start - v_advice_date;
            SELECT min(id) INTO v_moaa_table_id
                 FROM (
                     SELECT id, datee, advice_dt_cur
                     FROM main_ori_aero_advice_table mo
                     WHERE  mo.dubp_code = v_dubt_code
                     AND advice_dt_cur <= v_date_start
                 ) WHERE advice_dt_cur is not null AND datee = v_advice_date;
         END IF;
         UPDATE pma_data_set ds
             SET  ds.last_adv_day_cnt = max_adv_day_cnt 
                 ,ds.last_adv_day_cnt_3 = max_adv_day_cnt
                 --,ds.moaa_table_id = v_moaa_table_id --ds.last_adv_day_cnt = max_adv_day_cnt, ds.last_adv_day_cnt_3 = max_adv_day_cnt
             WHERE ds.models_id = p_model_id
             AND ds.short_dubt_code_id = v_short_dubt_code_id
             AND ds.models_date_num = 1;
      END LOOP;
      CLOSE cur01_all_dubt_code1;
      COMMIT;

      -- cur02_all_group_code2
      v_moaa_table_id := 0;
      OPEN cur02_all_group_code2;
      LOOP
         FETCH cur02_all_group_code2 INTO v_group_code_id;
         EXIT WHEN cur02_all_group_code2%NOTFOUND;

         v_advice_date := NULL;
         SELECT MAX(nvl(mo.advice_dt_cur, TO_DATE('01-01-0001', 'DD-MM-YYYY'))) INTO v_advice_date
            FROM main_ori_aero_advice_table mo
            WHERE advice_dt_cur <= v_date_start
              AND mo.dubp_code IN
                 (select dv.value DUBP_CODE   --, ds.id group_id, ds.subset_name DUBP_GROUP
                     from directory_subsets ds, directory_subset_values dsv, directory_values dv
                     where ds.subset_directory_id=-1710
--                     and ds.id in(-1711,-1712,-1713,-1714,-1715,-1716,-1717,-1718,-1719,-1720)
                     and ds.id = dsv.value_subset_id
                     and dsv.value_id = dv.id
                     AND ds.id = v_group_code_id);

         IF v_advice_date IS NULL THEN
            max_adv_day_cnt := 999999;
            v_moaa_table_id := -1;
         ELSE
            max_adv_day_cnt := v_date_start - v_advice_date;            
            SELECT min(mo.id) INTO v_moaa_table_id
            FROM main_ori_aero_advice_table mo
            WHERE advice_dt_cur <= v_date_start
              AND mo.advice_dt_cur = v_advice_date
              AND mo.dubp_code IN
                 (select dv.value DUBP_CODE   --, ds.id group_id, ds.subset_name DUBP_GROUP
                     from directory_subsets ds, directory_subset_values dsv, directory_values dv
                     where ds.subset_directory_id=-1710
--                     and ds.id in(-1711,-1712,-1713,-1714,-1715,-1716,-1717,-1718,-1719,-1720)
                     and ds.id = dsv.value_subset_id
                     and dsv.value_id = dv.id
                     AND ds.id = v_group_code_id); --v_group_code_id);
            
         END IF;
         UPDATE pma_data_set ds
             SET ds.last_adv_day_cnt = max_adv_day_cnt
                ,ds.last_adv_day_cnt_3 = max_adv_day_cnt 
                --,ds.moaa_table_id = v_moaa_table_id   --ds.last_adv_day_cnt = max_adv_day_cnt
             WHERE ds.models_id = p_model_id
             AND ds.group_code_id = v_group_code_id
             AND ds.models_date_num = 1;

      END LOOP;
      CLOSE cur02_all_group_code2;
      COMMIT;

   END load_first_advice_day_cnt_2;


   -- =============================================================================================
   -- || Point-fill last_adv_day_cnt = 0 of main_ori_aero_advice_table Put only 0 ||
   -- =============================================================================================
   PROCEDURE load_advice_day_cnt0_2(p_model_id INTEGER) IS
      v_short_dubt_code_id INTEGER;
      v_group_code_id      INTEGER;
      v_moaa_table_id      INTEGER;
      
      --v_last_adv_day_cnt   INTEGER;
      v_dubt_code          VARCHAR2(1000);
      v_date_start         DATE;
      v_date_end           DATE;
      v_advice_dt_cur      DATE;
      --v_datee              DATE;

      CURSOR cur01_all_dubt_code1 IS SELECT DISTINCT  md.short_dubt_code_id, md.dubp_code
         FROM model_data_set md
         WHERE md.models_id = p_model_id
            AND md.code_group IS  NULL ;   --AND md.short_dubt_code_id = 1472
         -- ORDER BY md.short_dubt_code_id;

      CURSOR cur02_all_group_code2 IS SELECT DISTINCT ds.group_code_id
         FROM pma_data_set ds WHERE ds.models_id = p_model_id
         AND ds.group_code_id IS NOT NULL ORDER BY ds.group_code_id;
         
      CURSOR cur03_all_advice_dt_cur IS SELECT mo.advice_dt_cur  -- Получилось епреемлемо долго
                  FROM main_ori_aero_advice_table mo
                  WHERE mo.advice_dt_cur IS NOT NULL
                    AND mo.advice_dt_cur >= v_date_start
                    AND mo.advice_dt_cur <= v_date_end
                    AND mo.dubp_code IN
                 (select dv.value DUBP_CODE   --, ds.id group_id, ds.subset_name DUBP_GROUP
                     from directory_subsets ds, directory_subset_values dsv, directory_values dv
                     where ds.subset_directory_id=-1710
--                     and ds.id in(-1711,-1712,-1713,-1714,-1715,-1716,-1717,-1718,-1719,-1720)
                     and ds.id = dsv.value_subset_id
                     and dsv.value_id = dv.id
                     AND ds.id = v_group_code_id
                 );   

   BEGIN

      SELECT TO_DATE(pm.date_start, 'YYYY-MM-DD'), TO_DATE(pm.date_end, 'YYYY-MM-DD') INTO v_date_start, v_date_end
         FROM pawlin_models pm WHERE pm.id = p_model_id;

      OPEN cur01_all_dubt_code1;
      LOOP
         FETCH cur01_all_dubt_code1 INTO v_short_dubt_code_id, v_dubt_code;
         EXIT WHEN cur01_all_dubt_code1%NOTFOUND;
         v_moaa_table_id := -1;

         SELECT max(at.advice_dt_cur) INTO v_advice_dt_cur
            FROM main_ori_aero_advice_table at
            WHERE at.DUBP_CODE_ID = v_short_dubt_code_id
            AND at.advice_dt_cur <= v_date_start;
         

         IF v_advice_dt_cur IS NULL THEN
            v_moaa_table_id := -2;
         ELSE
           -- Set 0 for days when there were events.
           UPDATE pma_data_set ds
             SET ds.last_adv_day_cnt = 0, ds.last_adv_day_cnt_3 = 0 
             WHERE ds.models_id = p_model_id AND ds.short_dubt_code_id = v_short_dubt_code_id  AND ds.datee IN
               (SELECT mo.advice_dt_cur
                   FROM main_ori_aero_advice_table mo
                   WHERE mo.advice_dt_cur IS NOT NULL
                     AND mo.advice_dt_cur >= v_date_start
                     AND mo.advice_dt_cur <= v_date_end
                     AND mo.DUBP_CODE_ID = v_short_dubt_code_id
                );
                
         END IF;  
         COMMIT;
      END LOOP;
      CLOSE cur01_all_dubt_code1;

      OPEN cur02_all_group_code2;
      LOOP
         FETCH cur02_all_group_code2 INTO v_group_code_id;
         EXIT WHEN cur02_all_group_code2%NOTFOUND;
         
         UPDATE pma_data_set ds SET ds.last_adv_day_cnt = 0, ds.last_adv_day_cnt_3 = 0
            WHERE ds.models_id = p_model_id AND ds.group_code_id = v_group_code_id
              AND ds.datee IN
              (SELECT mo.advice_dt_cur
                  FROM main_ori_aero_advice_table mo
                  WHERE mo.advice_dt_cur IS NOT NULL
                    AND mo.advice_dt_cur >= v_date_start
                    AND mo.advice_dt_cur <= v_date_end
                    AND mo.dubp_code IN
                 (select dv.value DUBP_CODE   --, ds.id group_id, ds.subset_name DUBP_GROUP
                     from directory_subsets ds, directory_subset_values dsv, directory_values dv
                     where ds.subset_directory_id=-1710
--                     and ds.id in(-1711,-1712,-1713,-1714,-1715,-1716,-1717,-1718,-1719,-1720)
                     and ds.id = dsv.value_subset_id
                     and dsv.value_id = dv.id
                     AND ds.id = v_group_code_id
                 )
              );
      COMMIT;
      END LOOP;
      CLOSE cur02_all_group_code2;

      --commit;
   END load_advice_day_cnt0_2;


   FUNCTION get_dubp_code(p_model_id INTEGER, p_short_dubp_code_id IN INTEGER) RETURN VARCHAR2 IS
     v_dubp_code VARCHAR2(1000);
   BEGIN
      SELECT min(mds.dubp_code) INTO v_dubp_code
         FROM model_data_set mds
         WHERE mds.models_id = p_model_id
         AND mds.short_dubt_code_id = p_short_dubp_code_id
         GROUP BY mds.short_dubt_code_id;
      RETURN v_dubp_code;
   END get_dubp_code;



   FUNCTION get_short_dubp_code_id(p_model_id INTEGER, p_dubp_code IN VARCHAR2) RETURN INTEGER IS
     v_short_dubp_code_id INTEGER;
   BEGIN
      SELECT min(mds.short_dubt_code_id) INTO v_short_dubp_code_id
         FROM model_data_set mds
         WHERE mds.models_id = p_model_id
         AND mds.dubp_code = p_dubp_code
         GROUP BY mds.short_dubt_code_id;
      RETURN v_short_dubp_code_id;
   END get_short_dubp_code_id;


   -- write down short / medium / long
   PROCEDURE updt_adv_dist_category(p_model_id INTEGER) IS
      max_adv_day_cnt      INTEGER;
      v_short_dubt_code_id INTEGER;
      v_group_code_id      INTEGER;
      v_dubt_code          VARCHAR2(1000);

      p_MAX_reasonable_adv_dist INTEGER := 180;  -- 0, 90; 180, 360, 700, 1000

      CURSOR cur01_all_dubt_code1 IS SELECT DISTINCT  md.short_dubt_code_id, md.dubp_code
         FROM model_data_set md
         WHERE md.models_id = p_model_id --AND md.short_dubt_code_id = 1472
         AND md.code_group IS     NULL ORDER BY md.short_dubt_code_id;
      CURSOR cur02_all_group_code2 IS SELECT DISTINCT ds.group_code_id
         FROM pma_data_set ds WHERE ds.models_id = p_model_id
         AND ds.group_code_id IS NOT NULL ORDER BY ds.group_code_id;

   BEGIN

      OPEN cur01_all_dubt_code1;
      LOOP
         FETCH cur01_all_dubt_code1 INTO v_short_dubt_code_id, v_dubt_code;
         EXIT WHEN cur01_all_dubt_code1%NOTFOUND;

         SELECT max(ds1.last_adv_day_cnt) INTO max_adv_day_cnt
            FROM pma_data_set ds1 WHERE ds1.models_id = p_model_id
            AND ds1.short_dubt_code_id = v_short_dubt_code_id;

         -- If ADV_DIST <= MAX_REASONABLE_ADV_DIST / 3 -> short, otherwise
         -- If ADV_DIST <= MAX_REASONABLE_ADV_DIST / 3 * 2 -> medium, otherwise long
         UPDATE pma_data_set ds
            SET ds.adv_dist_category =
               CASE
               WHEN  ds.last_adv_day_cnt <= p_MAX_reasonable_adv_dist / 3 THEN 'short'
               WHEN  ds.last_adv_day_cnt >  p_MAX_reasonable_adv_dist / 3
                 AND ds.last_adv_day_cnt <= p_MAX_reasonable_adv_dist / 3 * 2 THEN 'medium'
               ELSE 'long'
               END
            WHERE ds.models_id = p_model_id
              AND ds.short_dubt_code_id = v_short_dubt_code_id
              AND ds.short_dubt_code_id IS NOT NULL AND ds.group_code_id IS NULL;
          COMMIT;

      END LOOP;
      CLOSE cur01_all_dubt_code1;
      COMMIT;

      -- cur02_all_group_code2
      OPEN cur02_all_group_code2;
      LOOP
         FETCH cur02_all_group_code2 INTO v_group_code_id;
         EXIT WHEN cur02_all_group_code2%NOTFOUND;

         SELECT max(ds1.last_adv_day_cnt) INTO max_adv_day_cnt
            FROM pma_data_set ds1 WHERE ds1.models_id = p_model_id
            AND ds1.group_code_id = v_group_code_id;

         -- Если ADV_DIST < MAX_REASONABLE_ADV_DIST / 3 -> short, otherwise
         -- Если ADV_DIST < MAX_REASONABLE_ADV_DIST / 2 -> medium, otherwise long
         UPDATE pma_data_set ds
            SET ds.adv_dist_category =
               CASE
               WHEN  ds.last_adv_day_cnt <= p_MAX_reasonable_adv_dist / 3 THEN 'short'
               WHEN  ds.last_adv_day_cnt >  p_MAX_reasonable_adv_dist / 3
                 AND ds.last_adv_day_cnt <= p_MAX_reasonable_adv_dist / 3 * 2 THEN 'medium'
               ELSE 'long'
               END
            WHERE ds.models_id = p_model_id
              AND ds.group_code_id = v_group_code_id
              AND ds.group_code_id IS NOT NULL;
            COMMIT;

      END LOOP;
      CLOSE cur02_all_group_code2;
      COMMIT;

   END updt_adv_dist_category;



   PROCEDURE load_pma_result_3 (p_model_id INTEGER) IS

	    v_grouped_or_not     CHAR(1) := 'N';
    	 v_count_period       INTEGER;
       v_count_period_max   INTEGER;
   	 v_count_period_from  INTEGER;
       v_count2             INTEGER;
  	    v_count_period_to    INTEGER; -- We consider the schedule to this day.
  	    v_short_events       INTEGER;
	    v_short_days         INTEGER;
	    v_short_intens      FLOAT;
	    v_medium_events      INTEGER;
	    v_medium_days        INTEGER;
	    v_medium_intens     FLOAT;
	    v_long_events        INTEGER;
	    v_long_days          INTEGER;
	    v_long_intens       FLOAT;
	    v_excluded_prs_start INTEGER;
	    v_excluded_prs_end   INTEGER;
	    v_increase_num       INTEGER;
       v_count_1            INTEGER;
       v_events_total       INTEGER;

       v_short_dubt_code_id INTEGER;
       v_group_code_id      INTEGER;
       j2                   INTEGER;

       v_k_sum            FLOAT;
       k_1                FLOAT; -- k_1 - less often, k_3 - more often.
       k_3                FLOAT;

       CURSOR cur01_all_dubt_code1 IS SELECT ds.short_dubt_code_id, max(ds.last_adv_day_cnt)
          FROM pma_data_set ds
          WHERE ds.models_id = p_model_id AND ds.group_code_id IS NULL AND ds.last_adv_day_cnt <= 365 * 5
          GROUP BY ds.short_dubt_code_id;--  ORDER BY 1;

      CURSOR cur02_all_group_code2 IS SELECT ds.group_code_id, max(ds.last_adv_day_cnt)
          FROM pma_data_set ds
          WHERE ds.models_id = p_model_id AND ds.group_code_id IS NOT NULL AND ds.last_adv_day_cnt <= 365 * 5
          GROUP BY ds.group_code_id;

      CURSOR cur03_all_code_id3 IS SELECT rs.code_id, rs.grouped_or_not, max(rs.increase_num)
          FROM pma_result3 rs
          WHERE rs.models_id = p_model_id AND rs.excluded_prs_end + rs.excluded_prs_start < 25
          GROUP BY rs.code_id, rs.grouped_or_not;

      CURSOR cur04_all_code_id4 IS SELECT rs.code_id, rs.grouped_or_not, max(rs.main_rec)
          FROM pma_result3 rs
          WHERE rs.models_id = p_model_id
          GROUP BY rs.code_id, rs.grouped_or_not
          HAVING max(rs.main_rec) = -1;


   BEGIN


      OPEN cur01_all_dubt_code1;
      LOOP
         FETCH cur01_all_dubt_code1 INTO v_short_dubt_code_id, v_count_period_max;
         EXIT WHEN cur01_all_dubt_code1%NOTFOUND;
         IF v_count_period_max >= 60 THEN
            v_count2 := 1;
         ELSE
            v_count2 := 0;
         END IF;

         SELECT nvl(sum(ds.event_fact), -1) INTO v_events_total
                  FROM pma_data_set ds
                  WHERE ds.models_id = p_model_id   AND ds.short_dubt_code_id = v_short_dubt_code_id;

         FOR i1 IN 0..1
         LOOP
            v_count_period_from := i1 * 30;
            v_count2 := (v_count_period_max - v_count_period_from) / 30;
            IF v_count2 > 10 THEN
               v_count2 := 10;
            END IF;

            j2 := 0;
            v_excluded_prs_end := 0;
            WHILE j2 <= v_count2 AND v_excluded_prs_end <= 20 LOOP
               v_count_period := floor((v_count_period_max - v_count_period_from - j2 * 30) / 3);
               v_count_period_to := v_count_period_from + v_count_period * 3;

               -- short
               SELECT nvl(sum(ds.event_fact), 0) INTO v_short_events
                  FROM pma_data_set ds
                  WHERE ds.models_id = p_model_id   AND ds.short_dubt_code_id = v_short_dubt_code_id
                    AND ds.last_adv_day_cnt >= v_count_period_from + v_count_period * 0
                    AND ds.last_adv_day_cnt <  v_count_period_from + v_count_period * 1;
               SELECT nvl(count(*), 0) INTO v_short_days
                  FROM pma_data_set ds
                  WHERE ds.models_id = p_model_id   AND ds.short_dubt_code_id = v_short_dubt_code_id
                    AND ds.last_adv_day_cnt >= v_count_period_from + v_count_period * 0
                    AND ds.last_adv_day_cnt <  v_count_period_from + v_count_period * 1;
               IF v_short_days = 0 THEN
                  v_short_intens := -1f;
               ELSE
                  v_short_intens := v_short_events * 30 / v_short_days;
               END IF;

               -- medium
               SELECT nvl(sum(ds.event_fact), 0) INTO v_medium_events
                  FROM pma_data_set ds
                  WHERE ds.models_id = p_model_id   AND ds.short_dubt_code_id = v_short_dubt_code_id
                    AND ds.last_adv_day_cnt >= v_count_period_from + v_count_period * 1
                    AND ds.last_adv_day_cnt <  v_count_period_from + v_count_period * 2;
               SELECT nvl(count(*), 0) INTO v_medium_days
                  FROM pma_data_set ds
                  WHERE ds.models_id = p_model_id   AND ds.short_dubt_code_id = v_short_dubt_code_id
                    AND ds.last_adv_day_cnt >= v_count_period_from + v_count_period * 1
                    AND ds.last_adv_day_cnt <  v_count_period_from + v_count_period * 2;
               IF v_medium_days = 0 THEN
                  v_medium_intens := -1f;
               ELSE
                  v_medium_intens := v_medium_events * 30 / v_medium_days;
               END IF;

               -- long
               SELECT nvl(sum(ds.event_fact), 0) INTO v_long_events
                  FROM pma_data_set ds
                  WHERE ds.models_id = p_model_id   AND ds.short_dubt_code_id = v_short_dubt_code_id
                    AND ds.last_adv_day_cnt >= v_count_period_from + v_count_period * 2
                    AND ds.last_adv_day_cnt <  v_count_period_from + v_count_period * 3;
               SELECT nvl(count(*), 0) INTO v_long_days
                  FROM pma_data_set ds
                  WHERE ds.models_id = p_model_id   AND ds.short_dubt_code_id = v_short_dubt_code_id
                    AND ds.last_adv_day_cnt >= v_count_period_from + v_count_period * 2
                    AND ds.last_adv_day_cnt <  v_count_period_from + v_count_period * 3;
               IF v_long_days = 0 THEN
                  v_long_intens := -1f;
               ELSE
                  v_long_intens := v_long_events * 30 / v_long_days;
               END IF;

               -- -- v_count_1    v_excluded_prs_start  v_excluded_prs_end
               SELECT nvl(sum(ds.event_fact), 0) INTO v_count_1
                  FROM pma_data_set ds
                  WHERE ds.models_id = p_model_id   AND ds.short_dubt_code_id = v_short_dubt_code_id
                    AND ds.last_adv_day_cnt < v_count_period_from;
               v_excluded_prs_start := v_count_1 * 30 / v_events_total;
               SELECT nvl(sum(ds.event_fact), 0) INTO v_count_1
                  FROM pma_data_set ds
                  WHERE ds.models_id = p_model_id   AND ds.short_dubt_code_id = v_short_dubt_code_id
                    AND ds.last_adv_day_cnt >= v_count_period_to;
               v_excluded_prs_end := v_count_1 * 30 / v_events_total;

               -- calculate v_increase_num
               v_count_1 := 0;
               IF   0f <= v_short_intens AND v_short_intens < v_medium_intens THEN
                  --v_count_1 := 20;
                  k_1 := v_short_intens / v_medium_intens;
               ELSIF  0f < v_short_intens AND 0f < v_medium_intens THEN
                  k_1 := v_short_intens / v_medium_intens;
               ELSE
                  k_1 := 1f;
               END IF;

               IF  0f < v_medium_intens AND v_medium_intens < v_long_intens  THEN
                  --v_count_1 := v_count_1 + 20;
                  k_3 := v_medium_intens / v_long_intens;
               ELSIF 0f = v_medium_intens AND v_medium_intens < v_long_intens  THEN
                  --v_count_1 := v_count_1 + 20;
                  k_3 := 1f;
               ELSIF 0f < v_long_intens AND 0f < v_medium_intens THEN
                  k_3 := v_short_intens / v_medium_intens;
               ELSE
                  k_3 := 1f;
               END IF;

               IF 0f <= v_short_intens AND v_short_intens < v_medium_intens AND v_medium_intens < v_long_intens THEN
                 v_count_1 := 20;
               ELSIF 0f <= v_short_intens AND v_short_intens = v_medium_intens AND v_medium_intens < v_long_intens THEN
                 v_count_1 := 15;
               ELSIF 0f <= v_short_intens AND v_short_intens < v_medium_intens AND v_medium_intens = v_long_intens THEN
                 v_count_1 := 15;
               ELSIF 0f <= v_short_intens AND v_short_intens < v_medium_intens AND v_medium_intens > v_long_intens THEN
                 v_count_1 := 10;
               ELSIF 0f <= v_short_intens AND v_short_intens > v_medium_intens AND v_medium_intens < v_long_intens THEN
                 v_count_1 := 10;
               END IF;

               IF v_excluded_prs_start + v_excluded_prs_end = 0 THEN
                 v_count_1 := v_count_1 + 10;
               ELSIF v_excluded_prs_start + v_excluded_prs_end > 30 THEN
                 NULL;
               ELSE
                 v_count_1 := v_count_1 + (v_excluded_prs_start + v_excluded_prs_end) / 3;
               END IF;

               IF k_1 > 0f THEN
                  v_k_sum := (1 / k_1 + k_3) / 2 - 1;
               ELSE
                  v_k_sum := k_3 / 2 - 1;
               END IF;

               IF v_k_sum < 5f THEN
                  v_count_1 := v_count_1 + ROUND(v_k_sum / 10);
               ELSE
                  v_count_1 := v_count_1 + 10;
               END IF;

               v_increase_num := v_count_1;

               INSERT INTO pma_result3 ( models_id
							,code_id
							,grouped_or_not
							,count_period
							,count_period_from
							,count_period_to
							  ,short_events
							  ,short_days
							  ,short_intens
							,medium_events
							,medium_days
							,medium_intens
							  ,long_events
							  ,long_days
							  ,long_intens
							,excluded_prs_start
							,excluded_prs_end
							,increase_num
                     ,main_rec )
                VALUES(p_model_id
							,v_short_dubt_code_id
							,'N'
							,v_count_period
							,v_count_period_from
							,v_count_period_to
							  ,v_short_events
							  ,v_short_days
							  ,v_short_intens
							,v_medium_events
							,v_medium_days
							,v_medium_intens
							  ,v_long_events
							  ,v_long_days
							  ,v_long_intens
							,v_excluded_prs_start
							,v_excluded_prs_end
							,v_increase_num
                     ,-1);
               j2 := j2 + 1;
            END LOOP;
         END LOOP;
      END LOOP;
      CLOSE cur01_all_dubt_code1;
      COMMIT;

      -- cur02_all_group_code2
      OPEN cur02_all_group_code2;
      LOOP
         FETCH cur02_all_group_code2 INTO v_group_code_id, v_count_period_max;
         EXIT WHEN cur02_all_group_code2%NOTFOUND;

         IF v_count_period_max >= 60 THEN
            v_count2 := 1;
         ELSE
            v_count2 := 0;
         END IF;

         SELECT nvl(sum(ds.event_fact), -1) INTO v_events_total
                  FROM pma_data_set ds
                  WHERE ds.models_id = p_model_id   AND ds.group_code_id = v_group_code_id;

         FOR i1 IN 0..1
         LOOP
            v_count_period_from := i1 * 30;
            v_count2 := (v_count_period_max - v_count_period_from) / 30;
            IF v_count2 > 10 THEN
               v_count2 := 10;
            END IF;

            j2 := 0;
            v_excluded_prs_end := 0;
            WHILE j2 <= v_count2 AND v_excluded_prs_end <= 20 LOOP
               v_count_period := floor((v_count_period_max - v_count_period_from - j2 * 30) / 3);
               v_count_period_to := v_count_period_from + v_count_period * 3;

               -- short
               SELECT nvl(sum(ds.event_fact), 0) INTO v_short_events
                  FROM pma_data_set ds
                  WHERE ds.models_id = p_model_id   AND ds.group_code_id = v_group_code_id
                    AND ds.last_adv_day_cnt >= v_count_period_from + v_count_period * 0
                    AND ds.last_adv_day_cnt <  v_count_period_from + v_count_period * 1;
               SELECT nvl(count(*), 0) INTO v_short_days
                  FROM pma_data_set ds
                  WHERE ds.models_id = p_model_id   AND ds.group_code_id = v_group_code_id
                    AND ds.last_adv_day_cnt >= v_count_period_from + v_count_period * 0
                    AND ds.last_adv_day_cnt <  v_count_period_from + v_count_period * 1;
               IF v_short_days = 0 THEN
                  v_short_intens := -1f;
               ELSE
                  v_short_intens := v_short_events * 30 / v_short_days;
               END IF;

               -- medium
               SELECT nvl(sum(ds.event_fact), 0) INTO v_medium_events
                  FROM pma_data_set ds
                  WHERE ds.models_id = p_model_id   AND ds.group_code_id = v_group_code_id
                    AND ds.last_adv_day_cnt >= v_count_period_from + v_count_period * 1
                    AND ds.last_adv_day_cnt <  v_count_period_from + v_count_period * 2;
               SELECT nvl(count(*), 0) INTO v_medium_days
                  FROM pma_data_set ds
                  WHERE ds.models_id = p_model_id   AND ds.group_code_id = v_group_code_id
                    AND ds.last_adv_day_cnt >= v_count_period_from + v_count_period * 1
                    AND ds.last_adv_day_cnt <  v_count_period_from + v_count_period * 2;
               IF v_medium_days = 0 THEN
                  v_medium_intens := -1f;
               ELSE
                  v_medium_intens := v_medium_events * 30 / v_medium_days;
               END IF;

               -- long
               SELECT nvl(sum(ds.event_fact), 0) INTO v_long_events
                  FROM pma_data_set ds
                  WHERE ds.models_id = p_model_id   AND ds.group_code_id = v_group_code_id
                    AND ds.last_adv_day_cnt >= v_count_period_from + v_count_period * 2
                    AND ds.last_adv_day_cnt <  v_count_period_from + v_count_period * 3;
               SELECT nvl(count(*), 0) INTO v_long_days
                  FROM pma_data_set ds
                  WHERE ds.models_id = p_model_id   AND ds.group_code_id = v_group_code_id
                    AND ds.last_adv_day_cnt >= v_count_period_from + v_count_period * 2
                    AND ds.last_adv_day_cnt <  v_count_period_from + v_count_period * 3;
               IF v_long_days = 0 THEN
                  v_long_intens := -1f;
               ELSE
                  v_long_intens := v_long_events * 30 / v_long_days;
               END IF;

               -- -- v_count_1    v_excluded_prs_start  v_excluded_prs_end
               SELECT nvl(sum(ds.event_fact), 0) INTO v_count_1
                  FROM pma_data_set ds
                  WHERE ds.models_id = p_model_id   AND ds.group_code_id = v_group_code_id
                    AND ds.last_adv_day_cnt < v_count_period_from;
               v_excluded_prs_start := v_count_1 * 30 / v_events_total;
               SELECT nvl(sum(ds.event_fact), 0) INTO v_count_1
                  FROM pma_data_set ds
                  WHERE ds.models_id = p_model_id   AND ds.group_code_id = v_group_code_id
                    AND ds.last_adv_day_cnt >= v_count_period_to;
               v_excluded_prs_end := v_count_1 * 30 / v_events_total;

               -- calculate v_increase_num
               v_count_1 := 0;
               IF   0f <= v_short_intens AND v_short_intens < v_medium_intens THEN
                  k_1 := v_short_intens / v_medium_intens;
               ELSIF  0f < v_short_intens AND 0f < v_medium_intens THEN
                  k_1 := v_short_intens / v_medium_intens;
               ELSE
                  k_1 := 1f;
               END IF;

               IF  0f < v_medium_intens AND v_medium_intens < v_long_intens  THEN
                  --v_count_1 := v_count_1 + 20;
                  k_3 := v_medium_intens / v_long_intens;
               ELSIF 0f = v_medium_intens AND v_medium_intens < v_long_intens  THEN
                  --v_count_1 := v_count_1 + 20;
                  k_3 := 1f;
               ELSIF 0f < v_long_intens AND 0f < v_medium_intens THEN
                  k_3 := v_short_intens / v_medium_intens;
               ELSE
                  k_3 := 1f;
               END IF;

               IF 0f <= v_short_intens AND v_short_intens < v_medium_intens AND v_medium_intens < v_long_intens THEN
                 v_count_1 := 20;
               ELSIF 0f <= v_short_intens AND v_short_intens = v_medium_intens AND v_medium_intens < v_long_intens THEN
                 v_count_1 := 15;
               ELSIF 0f <= v_short_intens AND v_short_intens < v_medium_intens AND v_medium_intens = v_long_intens THEN
                 v_count_1 := 15;
               ELSIF 0f <= v_short_intens AND v_short_intens < v_medium_intens AND v_medium_intens > v_long_intens THEN
                 v_count_1 := 10;
               ELSIF 0f <= v_short_intens AND v_short_intens > v_medium_intens AND v_medium_intens < v_long_intens THEN
                 v_count_1 := 10;
               END IF;

               IF v_excluded_prs_start + v_excluded_prs_end = 0 THEN
                 v_count_1 := v_count_1 + 10;
               ELSIF v_excluded_prs_start + v_excluded_prs_end > 30 THEN
                 NULL;
               ELSE
                 v_count_1 := v_count_1 + 10 - (v_excluded_prs_start + v_excluded_prs_end) / 3;
               END IF;

               IF k_1 > 0f THEN
                  v_k_sum := (1 / k_1 + k_3) / 2 - 1;
               ELSE
                  v_k_sum := k_3 / 2 - 1;
               END IF;

               IF v_k_sum < 5f THEN
                  v_count_1 := v_count_1 + ROUND(v_k_sum / 10);
               ELSE
                  v_count_1 := v_count_1 + 10;
               END IF;

               v_increase_num := v_count_1;

               INSERT INTO pma_result3 ( models_id
							,code_id
							,grouped_or_not
							,count_period
							,count_period_from
							,count_period_to
							  ,short_events
							  ,short_days
							  ,short_intens
							,medium_events
							,medium_days
							,medium_intens
							  ,long_events
							  ,long_days
							  ,long_intens
							,excluded_prs_start
							,excluded_prs_end
							,increase_num
                     ,main_rec)
                VALUES(p_model_id
							,v_group_code_id
							,'Y'
							,v_count_period
							,v_count_period_from
							,v_count_period_to
							  ,v_short_events
							  ,v_short_days
							  ,v_short_intens
							,v_medium_events
							,v_medium_days
							,v_medium_intens
							  ,v_long_events
							  ,v_long_days
							  ,v_long_intens
							,v_excluded_prs_start
							,v_excluded_prs_end
							,v_increase_num
                     ,-1);
               j2 := j2 + 1;
            END LOOP;
         END LOOP;
      END LOOP;
      CLOSE cur02_all_group_code2;
      COMMIT;

      -- set 1-4 in main_rec for lines with progression
      OPEN cur03_all_code_id3;
      LOOP
         FETCH cur03_all_code_id3 INTO v_short_dubt_code_id, v_grouped_or_not, v_increase_num;
         EXIT WHEN cur03_all_code_id3%NOTFOUND;

          SELECT min(rs.count_period_from) INTO v_count_period_from
             FROM pma_result3 rs
             WHERE rs.models_id = p_model_id AND rs.excluded_prs_end + rs.excluded_prs_start < 25
               AND rs.code_id = v_short_dubt_code_id AND rs.grouped_or_not = v_grouped_or_not
               AND rs.increase_num = v_increase_num;

           SELECT max(rs.count_period) INTO v_count_period
              FROM pma_result3 rs
              WHERE rs.models_id = p_model_id AND rs.excluded_prs_end + rs.excluded_prs_start < 25
                AND rs.code_id = v_short_dubt_code_id AND rs.grouped_or_not = v_grouped_or_not
                AND rs.increase_num = v_increase_num AND rs.count_period_from = v_count_period_from;

            UPDATE pma_result3 rs SET rs.main_rec = v_increase_num
              WHERE rs.models_id = p_model_id --AND rs.excluded_prs_end + rs.excluded_prs_start < 25
                AND rs.code_id = v_short_dubt_code_id AND rs.grouped_or_not = v_grouped_or_not
                AND rs.increase_num = v_increase_num AND rs.count_period_from = v_count_period_from
                AND rs.count_period = v_count_period;

      END LOOP;
      CLOSE cur03_all_code_id3;
      COMMIT;

      -- set 0 in main_rec for 1 line where there is no progress.
      OPEN cur04_all_code_id4;
      LOOP
         FETCH cur04_all_code_id4 INTO v_short_dubt_code_id, v_grouped_or_not, v_increase_num;
         EXIT WHEN cur04_all_code_id4%NOTFOUND;

          SELECT min(rs.count_period_from) INTO v_count_period_from
             FROM pma_result3 rs
             WHERE rs.models_id = p_model_id
               AND rs.code_id = v_short_dubt_code_id AND rs.grouped_or_not = v_grouped_or_not;
               --AND rs.increase_num = v_increase_num;

           SELECT max(rs.count_period) INTO v_count_period
              FROM pma_result3 rs
              WHERE rs.models_id = p_model_id
                AND rs.code_id = v_short_dubt_code_id AND rs.grouped_or_not = v_grouped_or_not
                --AND rs.increase_num = v_increase_num
                AND rs.count_period_from = v_count_period_from;

            UPDATE pma_result3 rs SET rs.main_rec = 0
              WHERE rs.models_id = p_model_id
                AND rs.code_id = v_short_dubt_code_id AND rs.grouped_or_not = v_grouped_or_not
                AND rs.count_period_from = v_count_period_from
                AND rs.count_period = v_count_period;

      END LOOP;
      CLOSE cur04_all_code_id4;
      COMMIT;

   END load_pma_result_3;


   PROCEDURE load_final_k (p_model_id INTEGER) IS

   BEGIN

      INSERT INTO pma_code_and_k (models_id, short_dubt_code_id, k_minus, k_pluss)
         SELECT p_model_id, rs.code_id,
            CASE
               WHEN rs.short_intens <= 0f OR  rs.medium_intens <= 0f THEN 1
               ELSE rs.short_intens / rs.medium_intens
            END k_minus,
         CASE
               WHEN rs.long_intens <= 0f OR  rs.medium_intens <= 0f THEN 1
               ELSE rs.long_intens / rs.medium_intens
         END k_pluss
      FROM pma_result3 rs
      WHERE rs.models_id = p_model_id AND rs.grouped_or_not = 'N' AND rs.main_rec > 1
        AND ((rs.short_intens > 0f AND rs.medium_intens > 0f)  OR
              (rs.long_intens > 0f AND rs.medium_intens > 0f));

      INSERT INTO pma_code_and_k_final (models_id, short_dubt_code_id, k_minus, k_pluss)
         SELECT p_model_id, rs.code_id,
            CASE
               WHEN rs.short_intens <= 0f OR  rs.medium_intens <= 0f THEN 1
               ELSE rs.short_intens / rs.medium_intens
            END k_minus,
         CASE
               WHEN rs.long_intens <= 0f OR  rs.medium_intens <= 0f THEN 1
               ELSE rs.long_intens / rs.medium_intens
         END k_pluss
      FROM pma_result3 rs
      WHERE rs.models_id = p_model_id AND rs.grouped_or_not = 'N' AND rs.main_rec > 1
        AND ((rs.short_intens > 0f AND rs.medium_intens > 0f)  OR
              (rs.long_intens > 0f AND rs.medium_intens > 0f));

      ----  Now for on the record in
      INSERT INTO pma_code_and_k (models_id, short_dubt_code_id, group_code_id, k_minus, k_pluss)
         WITH mdl_data_set AS (SELECT DISTINCT dubp_code dubp_code, short_dubt_code_id FROM model_data_set WHERE models_id = 1483)
         SELECT p_model_id, mds.short_dubt_code_id, rs.code_id,
            CASE
               WHEN rs.short_intens <= 0f OR  rs.medium_intens <= 0f THEN 1
               ELSE rs.short_intens / rs.medium_intens
            END k_minus,
            CASE
            WHEN rs.long_intens <= 0f OR  rs.medium_intens <= 0f THEN 1
            ELSE rs.long_intens / rs.medium_intens
            END k_pluss
            FROM directory_subsets ds, directory_subset_values dsv, directory_values dv, pma_result3 rs , mdl_data_set mds
               where ds.subset_directory_id=-1710
              --       and ds.id in(-1711,-1712,-1713,-1714,-1715,-1716,-1717,-1718,-1719,-1720)
               and ds.id = dsv.value_subset_id
               and dsv.value_id = dv.id
               AND rs.code_id = ds.id AND dv.value = mds.dubp_code AND rs.models_id = p_model_id
               AND rs.grouped_or_not = 'Y' AND rs.main_rec > 1
               AND ((rs.short_intens > 0f AND rs.medium_intens > 0f)  OR
                     (rs.long_intens > 0f AND rs.medium_intens > 0f));
--
--                     AND ds.id IN (-1715, -1711);

      COMMIT;
   END load_final_k;
   
   
   -- Delete
   PROCEDURE run_all IS
      v_model_id INTEGER;
      CURSOR cur01_all_model_id IS SELECT pm.id FROM pawlin_models pm 
         WHERE pm.model_data_set_count = pm.view_data_set_count 
           AND pm.params_values_count = pm.view_values_count AND pm.learning_progress = 100
           AND pm.id NOT IN (SELECT DISTINCT ds.models_id FROM pma_data_set ds);
           --AND pm.id = 1497;  -- Удалить после отладки      
   BEGIN
      
      OPEN cur01_all_model_id;
      LOOP
         FETCH cur01_all_model_id INTO v_model_id;
         EXIT WHEN cur01_all_model_id%NOTFOUND;  
         
            run_this(v_model_id);
         
      END LOOP;
      CLOSE cur01_all_model_id;
   
   END;
   
   
   function get_dubp_advice_text(p_date in DATE, p_id in NUMBER, p_group_name IN VARCHAR2) return  varchar2 is
      res   varchar2(4000);
      v_res varchar2(500);
      CURSOR cur01 IS  select distinct substr(da.advice_text, 0, length(substr(advice_text,0,500))) || '..<br>' 
         from dubp_advices da 
            where da.advice_efficiency_id = -1813 
               and da.advice_status_id = 10005386 
               and da.is_active = 1
               and da.advice_object_id = p_id
               and (   nvl(da.advice_finish, da.advice_deadline) = p_date
                    OR                       da.advice_deadline  = p_date);
      --             
      CURSOR cur02 IS  select distinct substr(da.advice_text, 0, length(substr(advice_text,0,500))) || '..<br>' 
         from dubp_advices da 
            where da.advice_efficiency_id = -1813 
               and da.advice_status_id = 10005386 
               and da.is_active = 1
               and (   nvl(da.advice_finish, da.advice_deadline) = p_date
                    OR                       da.advice_deadline  = p_date )                     
               AND da.advice_object_id IN (SELECT mo.id FROM main_ori_aero_advice_table mo
                                              WHERE mo.advice_dt_cur = p_date
                                               AND INSTR(mo.CODE_GROUP, p_group_name) > 0
                                           );
   
     BEGIN
        IF p_group_name = '-' THEN
           res := ' ';
           OPEN cur01;
              LOOP
              FETCH cur01 INTO v_res;
              EXIT WHEN cur01%NOTFOUND;

              IF LENGTH(res) + LENGTH(v_res) < 4000 THEN
                 res := res || v_res;
              END IF;

              END LOOP;
           CLOSE cur01;
           IF LENGTH(res) < 10 THEN 
              res := 'Descriptions for the event with a code ' || p_id || ' no.';
           END IF;
       --res := "There is no description in the events table";
        ELSE 
           res := ' ';
           OPEN cur02;
              LOOP
              FETCH cur02 INTO v_res;
              EXIT WHEN cur02%NOTFOUND;

              IF LENGTH(res) + LENGTH(v_res) < 4000 THEN
                 res := res || v_res;
              END IF;

              END LOOP;
           CLOSE cur02;
           IF LENGTH(res) < 10 THEN 
              res := 'Descriptions for the event for the group ' || p_group_name || ' no.';           
           END IF;
        END IF;   
        
   RETURN res;
   END get_dubp_advice_text;
   

end pma_data_set_utils;
/
