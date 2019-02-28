CREATE OR REPLACE PACKAGE serv_pack IS

  -- ======================================================================================
  -- Basic procedure for complete passage through the entire table and recording all exceptions
  -- ======================================================================================
  PROCEDURE get_all_exceptions(p_ref_table_name   IN VARCHAR2,
                            p_ref_column_name  IN VARCHAR2,
                            p_src_table_name  IN VARCHAR2,
                            p_src_column_name IN VARCHAR2,
                            p_join_condition   IN VARCHAR2, 
                            p_compare_condition IN VARCHAR2) ;
                            
  -- =======================================================================
  -- Procedure for checking exceptions after parsing.
  -- =======================================================================
  PROCEDURE re_check_exceptions;

                            
  -- =======================================================================
  -- function to get select for REFCURSOR or count
  -- =======================================================================
  FUNCTION get_select_st (  p_select_or_count IN VARCHAR2, 
                            p_ref_table_name   IN VARCHAR2,    
                            p_ref_column_name  IN VARCHAR2,
                            p_src_table_name  IN VARCHAR2,
                            p_src_column_name IN VARCHAR2,
                            p_join_condition   IN VARCHAR2, 
                            p_compare_condition IN VARCHAR2 DEFAULT NULL) RETURN VARCHAR2;


  -- =======================================================================
  -- function to get the code all_errors.err_id (if necessary with INSERT)
  -- =======================================================================
  FUNCTION  get_err_id(p_ref_table_name   IN VARCHAR2,
                            p_ref_column_name  IN VARCHAR2,
                            p_src_table_name  IN VARCHAR2,
                            p_src_column_name IN VARCHAR2,
                            p_join_condition   IN VARCHAR2, 
                            p_compare_condition IN VARCHAR2) RETURN NUMBER;


  -- =======================================================================
  -- Procedure to generate (Inserts) all records all_errors_details (error codes)
  -- =======================================================================
  PROCEDURE set_all_err_det(p_err_id          IN VARCHAR2, 
                            p_compare_condition IN VARCHAR2);

							
  -- =======================================================================
  -- function to get compound all_errors_details.id (enum through,)
  -- =======================================================================
  FUNCTION  get_err_det_ids(p_id_xref         IN NUMBER,
                            p_ref_table_name  IN VARCHAR2,
                            p_ref_column_name IN VARCHAR2,
                            p_src_table_name  IN VARCHAR2,
                            p_src_column_name IN VARCHAR2,
                            p_join_condition  IN VARCHAR2, 
                            p_err_id          IN NUMBER) RETURN VARCHAR2;

END serv_pack;
/


CREATE OR REPLACE PACKAGE BODY serv_pack IS
  -- v_all_rc SYS_REFCURSOR;
  -- CURSOR main IS SELECT id_xref, system_name, end_date, out_of_print_date FROM authors ath;
  
  
  -- ======================================================================================
  -- Basic procedure for complete passage through the entire table and recording all exceptions
  -- ======================================================================================
  PROCEDURE get_all_exceptions(p_ref_table_name   IN VARCHAR2,
                            p_ref_column_name  IN VARCHAR2,
                            p_src_table_name  IN VARCHAR2,
                            p_src_column_name IN VARCHAR2,
                            p_join_condition   IN VARCHAR2, 
                            p_compare_condition IN VARCHAR2) IS 
    v_err_id  NUMBER;
    sql_stmt  VARCHAR2(500);
    sql_stmt1 VARCHAR2(500);
    v_count   NUMBER;
    scan_cursor SYS_REFCURSOR;
    v_id_xref     NUMBER;
    v_src_key_val VARCHAR2(255);
  BEGIN    

    v_err_id := get_err_id(p_ref_table_name, p_ref_column_name, p_src_table_name, p_src_column_name, p_join_condition,  p_compare_condition);    
    sql_stmt := get_select_st('select', p_ref_table_name, p_ref_column_name, p_src_table_name, p_src_column_name, p_join_condition);    
    
    OPEN scan_cursor FOR sql_stmt;
    LOOP
      FETCH scan_cursor INTO v_id_xref, v_src_key_val;
      EXIT WHEN scan_cursor%notFound;
        -- We enter only  if ount () = 0, taking into account p_compare_condition
        sql_stmt1 := get_select_st('count', p_ref_table_name, p_ref_column_name, p_src_table_name, p_src_column_name, 
                        p_join_condition, p_compare_condition) || ' AND ref.id_xref = ' || v_id_xref;
        EXECUTE IMMEDIATE sql_stmt1 INTO v_count;
        IF v_count = 0 THEN         
           -- is there this entry allready? 
           SELECT count(*) INTO v_count 
              FROM all_exceptions 
              WHERE id_xref = v_id_xref AND err_id = v_err_id;
           IF v_count = 0 THEN 
           -- There is no antry. Lets insert
             INSERT INTO all_exceptions(id_xref, src_key_val, err_id, err_details_ids, create_date)
                VALUES(v_id_xref, v_src_key_val, v_err_id, 
                  get_err_det_ids(v_id_xref, p_ref_table_name, p_ref_column_name, p_src_table_name, p_src_column_name, p_join_condition, v_err_id), SYSDATE); 
           ELSE             
             -- Record exist, update err_details_ids
             UPDATE all_exceptions 
               SET err_details_ids = get_err_det_ids(v_id_xref, p_ref_table_name, p_ref_column_name, p_src_table_name, p_src_column_name, p_join_condition, v_err_id)
               WHERE id_xref = v_id_xref AND err_id = v_err_id;
           END IF;
        ELSE 
          -- Execute Delete statement in case this line was entered before.
          DELETE FROM all_exceptions WHERE id_xref = v_id_xref AND err_id = v_err_id;        
        END IF;      
    END LOOP;
    CLOSE scan_cursor;
    COMMIT;    
  END get_all_exceptions;
  
  
  -- =======================================================================
  -- Procedure for checking exceptions after parsing.
  -- =======================================================================
  PROCEDURE re_check_exceptions IS
     CURSOR c_all_exceptions IS SELECT id_xref, err_id FROM all_exceptions ORDER BY err_id;      
     v_id_xref   NUMBER;
	 v_err_id    NUMBER;
     v_err_id_ch NUMBER;   
     v_ref_table_name     VARCHAR2(100); 
     v_ref_column_name    VARCHAR2(100);
     v_src_table_name     VARCHAR2(100);
     v_src_column_name    VARCHAR2(100);
     v_join_condition     VARCHAR2(500);     
     sql_stmt    VARCHAR2(500);
  BEGIN
     v_err_id := -1;
     v_err_id_ch := 0;
     OPEN c_all_exceptions;
     LOOP 
       FETCH c_all_exceptions INTO v_id_xref, v_err_id;
       EXIT WHEN c_all_exceptions%NOTFOUND;
       -- 
       IF v_err_id <> v_err_id_ch THEN 
          SELECT   ref_table_name,   ref_column_name,   src_table_name,   src_column_name,   join_condition
            INTO v_ref_table_name, v_ref_column_name, v_src_table_name, v_src_column_name, v_join_condition
            FROM all_errors
              WHERE err_id = v_err_id;
          -- Avoid repeating the same query with the same err_id:
          v_err_id_ch := v_err_id;  
       END IF;       
       UPDATE all_exceptions 
           SET err_details_ids = get_err_det_ids(v_id_xref, v_ref_table_name, v_ref_column_name, v_src_table_name, v_src_column_name, v_join_condition, v_err_id)
           WHERE id_xref = v_id_xref AND err_id = v_err_id;
     END LOOP;
     CLOSE c_all_exceptions;
     COMMIT;
  END;  
  
    
  -- =======================================================================
  -- function to get select для REFCURSOR or count 
  -- ======================================================================= 
  FUNCTION get_select_st   (p_select_or_count   IN VARCHAR2, 
                            p_ref_table_name    IN VARCHAR2,    
                            p_ref_column_name   IN VARCHAR2,
                            p_src_table_name    IN VARCHAR2,
                            p_src_column_name   IN VARCHAR2,
                            p_join_condition    IN VARCHAR2, 
                            p_compare_condition IN VARCHAR2 DEFAULT NULL) RETURN VARCHAR2 IS                             
     my_select VARCHAR2(1000);
     v_data_type VARCHAR2(50);
   BEGIN
     IF p_select_or_count = 'count' THEN
        my_select := 'SELECT count(*) ';
     ELSE 
        SELECT data_type INTO v_data_type
           FROM USER_TAB_COLUMNS 
           WHERE TABLE_NAME = UPPER(p_src_table_name) AND COLUMN_NAME = UPPER(p_src_column_name);
        IF  v_data_type = 'VARCHAR2' THEN 
           my_select := 'SELECT ref.id_xref id_xref, src.' || p_src_column_name || ' src_key_val ';
        ELSIF v_data_type = 'DATE' THEN  
           my_select := 'SELECT ref.id_xref id_xref, ' || 
            ' TO_CHAR(src.' ||  p_src_column_name  || ', ' || '''' || 'YYYY-MM-DD' || '''' || ') src_key_val ';
        ELSE
           my_select := 'SELECT ref.id_xref id_xref, ' || ' TO_CHAR(src.' ||  p_src_column_name  ||  ') src_key_val ';
        END IF;
     END IF;
     my_select := my_select || ' FROM ' || p_ref_table_name || ' ref, ' || p_src_table_name || ' src ' ||
                     ' WHERE ref.' || p_ref_column_name || ' = src.' || p_src_column_name || ' AND ' || p_join_condition;                         
     IF  p_compare_condition IS NOT NULL THEN 
         my_select := my_select || ' AND ' || p_compare_condition;
     END IF;                 
                      
     RETURN my_select;
   END get_select_st;
   
  -- =======================================================================
  -- function to get the error code (if necessary with INSERT
  -- =======================================================================

  FUNCTION get_err_id(p_ref_table_name   IN VARCHAR2,
                            p_ref_column_name  IN VARCHAR2,
                            p_src_table_name  IN VARCHAR2,
                            p_src_column_name IN VARCHAR2,
                            p_join_condition   IN VARCHAR2, 
                            p_compare_condition IN VARCHAR2) RETURN NUMBER IS 
      
    v_count NUMBER;
    v_err_id NUMBER;
  BEGIN    
    v_count := 0;
    SELECT count(*) INTO v_count 
      FROM all_errors 
      WHERE ref_table_name = p_ref_table_name 
        AND ref_column_name = p_ref_column_name
        AND src_table_name = p_src_table_name
        AND src_column_name = p_src_column_name
        AND join_condition = p_join_condition
        AND compare_condition = p_compare_condition;  
        
     IF v_count = 0 THEN       
       SELECT all_errors_sq1.Nextval INTO v_err_id FROM DUAL;
       INSERT INTO all_errors(err_id, ref_table_name, ref_column_name, src_table_name, src_column_name, join_condition, compare_condition) 
          VALUES (v_err_id, p_ref_table_name, p_ref_column_name, p_src_table_name, p_src_column_name, p_join_condition, p_compare_condition);
       COMMIT;    
       set_all_err_det(v_err_id, p_compare_condition);
     ELSE        
       SELECT err_id INTO v_err_id 
         FROM all_errors 
      WHERE ref_table_name = p_ref_table_name 
        AND ref_column_name = p_ref_column_name
        AND src_table_name = p_src_table_name
        AND src_column_name = p_src_column_name
        AND join_condition = p_join_condition
        AND compare_condition = p_compare_condition;
      COMMIT;
     END IF;   
     RETURN v_err_id;
  END get_err_id;

  -- =======================================================================
  -- Procedure to generate (Inserts) all records all_errors_details (error codes)
  -- =======================================================================
  PROCEDURE set_all_err_det(p_err_id          IN VARCHAR2, 
                            p_compare_condition IN VARCHAR2) IS                             

     v_single_cc VARCHAR2(500);
     v_rest_cc   VARCHAR2(500);     
     v_position  NUMBER;
  BEGIN 
     v_rest_cc  := LOWER(p_compare_condition);
     LOOP
       v_position := INSTR(v_rest_cc, 'and');
    
       IF v_position > 0 THEN
         v_single_cc := SUBSTR(v_rest_cc, 1, v_position - 2);
         v_rest_cc := SUBSTR(v_rest_cc, v_position + 4);
         INSERT INTO all_errors_details (err_id, error_condition) VALUES(p_err_id, v_single_cc);
       ELSE
         INSERT INTO all_errors_details (err_id, error_condition) VALUES(p_err_id, v_rest_cc);
         EXIT;
       END IF;       
    END LOOP;
    COMMIT;                 
  END set_all_err_det;
  
  
  -- =======================================================================
  -- function to get compound all_errors_details.id (enum through,)
  -- =======================================================================
  FUNCTION  get_err_det_ids(p_id_xref         IN NUMBER,
                            p_ref_table_name  IN VARCHAR2,
                            p_ref_column_name IN VARCHAR2,
                            p_src_table_name  IN VARCHAR2,
                            p_src_column_name IN VARCHAR2,
                            p_join_condition  IN VARCHAR2, 
                            p_err_id          IN NUMBER) RETURN VARCHAR2 IS 
                            
    v_all_exc_ids VARCHAR2(255);
    CURSOR c_errors_details (v_err_id NUMBER) IS SELECT id, error_condition FROM all_errors_details WHERE err_id = v_err_id;
    v_id  NUMBER;
    v_error_condition VARCHAR2(255);
    sql_stmt_main    VARCHAR2(500);
    sql_stmt_current VARCHAR2(500);
    v_count NUMBER;
  BEGIN
    v_all_exc_ids := 'OK';
    sql_stmt_main := get_select_st('count', p_ref_table_name, p_ref_column_name, p_src_table_name, p_src_column_name, p_join_condition);
     OPEN c_errors_details(p_err_id);
     LOOP 
       FETCH c_errors_details INTO v_id, v_error_condition;
       EXIT WHEN c_errors_details%NOTFOUND;
       -- We check each condition separately:
       sql_stmt_current :=  sql_stmt_main || ' AND ' || v_error_condition || ' AND ref.id_xref = ' || p_id_xref;
       EXECUTE IMMEDIATE sql_stmt_current INTO v_count;
       IF v_count = 0 THEN
         IF v_all_exc_ids = 'OK' THEN  
            v_all_exc_ids := TO_CHAR(v_id);
         ELSE
            v_all_exc_ids := v_all_exc_ids || ', ' || TO_CHAR(v_id);
         END IF;          
       END IF;
     END LOOP;
     CLOSE c_errors_details;
     RETURN v_all_exc_ids;
  END get_err_det_ids;
  
END serv_pack;
/
