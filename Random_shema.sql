begin dbms_output.put_line('BP-110');
end;
/


create table params_for_models
(
  param_id      INTEGER,
  param_name    VARCHAR2(256) not null,
  param_numbers VARCHAR2(256)
) tablespace USERS;


COMMENT ON TABLE  params_for_models                IS 'Параметры для модели';
COMMENT ON COLUMN params_for_models.param_id       IS 'Generated ID';
COMMENT ON COLUMN params_for_models.param_name     IS 'Parameter name';
COMMENT ON COLUMN params_for_models.param_numbers  IS 'The numeric expression that we write in VARCHAR2';


ALTER TABLE params_for_models ADD CONSTRAINT params_for_models_pk primary key (param_id);
ALTER TABLE params_for_models ADD CONSTRAINT params_for_models_uq1 UNIQUE (param_name);

create sequence params_for_models_sq1 start with 1 increment by 1 NOCACHE;

CREATE OR REPLACE TRIGGER params_for_models_bi  BEFORE INSERT ON params_for_models
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
Begin
  IF :NEW.param_id IS NULL THEN 
    SELECT params_for_models_sq1.nextval INTO :NEW.param_id FROM SYS.DUAL;
  END IF;   
End;
/


create table params_values (
  id      			INTEGER,
  param_id      INTEGER,  
  models_id     INTEGER not null,
  param_value   VARCHAR2(256), 
  count_num  		INTEGER   
) tablespace USERS;

COMMENT ON TABLE  params_values 			IS 'Parameter values for models';
COMMENT ON COLUMN params_values.id    		IS 'ID values. From model_data_set, which was added as ORA_HASH from MAIN_ORI_AERO_ADVICE_TABLE';
COMMENT ON COLUMN params_values.param_id    IS 'ID parameter';
COMMENT ON COLUMN params_values.models_id   IS 'ID model';
COMMENT ON COLUMN params_values.param_value IS 'List of parameters in DB';
COMMENT ON COLUMN params_values.count_num   IS 'Repeat number of this parameter';


ALTER TABLE params_values ADD CONSTRAINT params_values_pk primary key (id, param_id, models_id );
ALTER TABLE params_values ADD CONSTRAINT params_values_fk1 FOREIGN KEY (param_id) REFERENCES params_for_models (param_id) on delete cascade;
ALTER TABLE params_values ADD CONSTRAINT params_values_fk2 FOREIGN KEY (models_id) REFERENCES pawlin_models (id) on delete cascade;


    
CREATE OR REPLACE VIEW PARAMS_VALUES_V AS
SELECT m.param_name, v.id, v.param_id, v.param_value, v.models_id, v.count_num
  FROM params_for_models m, params_values v
    WHERE m.param_id = v.param_id
    ORDER BY v.models_id, m.param_name;
    


create sequence pawlin_details_sq1 start with 1 increment by 1 NOCACHE;


CREATE OR REPLACE TRIGGER pawlin_details_bi  BEFORE INSERT ON pawlin_details
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
Begin
  IF :NEW.id IS NULL THEN 
    SELECT pawlin_details_sq1.nextval INTO :NEW.id FROM SYS.DUAL;
  END IF;   
End;
/


create table model_data_set (
  models_id     	INTEGER 			 DEFAULT  -1  NOT NULL ,
  datee				DATE											  NOT NULL ,
  day_num_t 		INTEGER				 DEFAULT  -1  NOT NULL ,
  year_num_tl	  	INTEGER 			 DEFAULT  -1  NOT NULL ,
  type_vs_code    	VARCHAR2(255)  DEFAULT '-1' NOT NULL ,
  type_vs_code_ID	INTEGER 			 DEFAULT  -1  NOT NULL ,
  INCIDENT_TYPE		VARCHAR2(1000) DEFAULT '-1' NOT NULL , 
  incident_type_short VARCHAR2(255)  DEFAULT '-1' NOT NULL ,  
  incident_t_short_ID INTEGER 			 DEFAULT  -1  NOT NULL ,
  dubp_code			  VARCHAR2(1000) DEFAULT '-1' NOT NULL ,
  short_dubp_code     VARCHAR2(255)  DEFAULT '-1' NOT NULL ,
  short_dubt_code_id  INTEGER 			 DEFAULT  -1  NOT NULL ,
  last_adv_day_cnt    INTEGER 			 DEFAULT  -1  NOT NULL ,
  klass_sob  		  INTEGER				 DEFAULT  -1  NOT NULL ,
  cnt_by_type         CHAR(1)        DEFAULT  'N' NOT NULL 
) tablespace USERS;



COMMENT ON TABLE  model_data_set 					IS 'Selected data table for the created model';
COMMENT ON COLUMN model_data_set.models_id    		IS 'models_id FK of course';
COMMENT ON COLUMN model_data_set.datee		    	IS 'DATEE - needed for the transition period for backward compatibility / minimizing code editing';
COMMENT ON COLUMN model_data_set.day_num_t 			IS 'Day from the beginning of the year';
COMMENT ON COLUMN model_data_set.year_num_tl  		IS 'Year';
COMMENT ON COLUMN model_data_set.type_vs_code  		IS 'Aircraft type';
COMMENT ON COLUMN model_data_set.type_vs_code_ID  	IS 'ID Aircraft type';
COMMENT ON COLUMN model_data_set.incident_type  	IS 'Incident type';
COMMENT ON COLUMN model_data_set.incident_type_short  IS 'Incident type - shortened. Until the first space';
COMMENT ON COLUMN model_data_set.incident_t_short_id  IS 'ID от Incident type - shortened. Until the first space';
COMMENT ON COLUMN model_data_set.short_dubp_code  		IS 'Treated, abbreviated dubp_code. It will need to be removed.';
COMMENT ON COLUMN model_data_set.short_dubt_code_id		IS 'ID shortened dubp_code from t. all_shorts_dubt_code';
COMMENT ON COLUMN model_data_set.klass_sob  			IS 'Apparently event class';
COMMENT ON COLUMN model_data_set.INCIDENT_TYPE    		IS 'INCIDENT_TYPE - needed for the transition period for backward compatibility minimizing code changes';
COMMENT ON COLUMN model_data_set.last_adv_day_cnt    	IS 'time in days between the event and the event related to this event';
COMMENT ON COLUMN model_data_set.dubp_code    			IS 'dubp_code - needed for the transition period for backward compatibility minimizing code changes';
COMMENT ON COLUMN model_data_set.cnt_by_type    		IS 'N - the risk report is constructed regardless of the type of aircraft, Y - depends';



ALTER TABLE model_data_set ADD CONSTRAINT model_data_set_fk1 FOREIGN KEY (models_id) 			 		REFERENCES pawlin_models (id) on delete cascade;


create table model_data_set_error (
  models_id     			INTEGER 			,
  datee								DATE					,
  day_num_t 					INTEGER				,
  year_num_tl	  			INTEGER 			,
  type_vs_code    		VARCHAR2(255) ,
  type_vs_code_ID		  INTEGER 			,
  INCIDENT_TYPE			  VARCHAR2(1000), 
  incident_type_short VARCHAR2(255) ,  
  incident_t_short_ID INTEGER 			,
  dubp_code						VARCHAR2(1000),
  short_dubp_code     VARCHAR2(255) ,
  short_dubt_code_id	INTEGER 			,
  last_adv_day_cnt    INTEGER 			,
  klass_sob  				 	INTEGER				
) tablespace USERS;


COMMENT ON TABLE  model_data_set_error 											IS 'Таблица c ошибками при загрузке. Заполняется триггером из model_data_set';


ALTER TABLE pawlin_models ADD model_data_set_count INTEGER DEFAULT 0;
ALTER TABLE pawlin_models ADD view_data_set_count  INTEGER DEFAULT 0;
ALTER TABLE pawlin_models ADD params_values_count  INTEGER DEFAULT 0;
ALTER TABLE pawlin_models ADD view_values_count    INTEGER DEFAULT 0;
ALTER TABLE pawlin_models ADD reports_status       INTEGER DEFAULT 0;
ALTER TABLE pawlin_models ADD learning_progress    INTEGER DEFAULT 0;


COMMENT ON COLUMN pawlin_models.model_data_set_count  					IS 'The number of entries in model_data_set from the load_mds_and_pvalues procedure';
COMMENT ON COLUMN pawlin_models.view_data_set_count  					  IS 'The number of records selected in the view';
COMMENT ON COLUMN pawlin_models.params_values_count  					  IS 'The number of parameters included in param_values from the load_mds_and_pvalues procedure';
COMMENT ON COLUMN pawlin_models.view_values_count  						  IS 'The number of parameters selected in the view';
COMMENT ON COLUMN pawlin_models.reports_status  	  					  IS 'Whether the report is prepared: 1: table is filled, 2: data is filled, -1: errors during the initial filling of the report table (codes), -2: error when filling with data from graphs';
COMMENT ON COLUMN pawlin_models.learning_progress               IS '% for which the model was trained. If <100 - do not give it to load.';



create table errors_log (
  error_date					DATE, 
  package_name				VARCHAR2(250), 
  description         VARCHAR2(1000) 
) tablespace USERS;

COMMENT ON TABLE  errors_log 							 IS 'Table for saving errors from procedures';
COMMENT ON COLUMN errors_log.error_date	    			 IS 'Sysdate mistakes';
COMMENT ON COLUMN errors_log.package_name   			 IS 'The package where the error occurred';
COMMENT ON COLUMN errors_log.description    			 IS 'Error message';


CREATE OR REPLACE TRIGGER model_data_set_bi  BEFORE INSERT ON model_data_set
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
Begin
  IF :NEW.models_id     			 IS NULL OR
     :NEW.datee								 IS NULL OR
     :NEW.day_num_t 					 IS NULL OR
     :NEW.year_num_tl	  			 IS NULL OR
     :NEW.type_vs_code    		 IS NULL OR 
     :NEW.type_vs_code_ID		   IS NULL OR
     :NEW.INCIDENT_TYPE			   IS NULL OR
     :NEW.incident_type_short  IS NULL OR
     :NEW.incident_t_short_ID  IS NULL OR
     :NEW.dubp_code						 IS NULL OR
     :NEW.short_dubp_code      IS NULL OR
     :NEW.short_dubt_code_id	 IS NULL OR 
     :NEW.last_adv_day_cnt     IS NULL OR
     :NEW.klass_sob  				 	 IS NULL 
  THEN
    INSERT INTO  model_data_set_error (
					 models_id 				   ,
      		 datee							 ,
      		 day_num_t 					 , 
					 year_num_tl	  		 ,   
      		 type_vs_code    		 , 
      		 type_vs_code_ID		 ,
      		 INCIDENT_TYPE			 ,
      		 incident_type_short ,
      		 incident_t_short_ID ,
      		 dubp_code					 ,
      		 short_dubp_code     ,
      		 short_dubt_code_id	 , 
      		 last_adv_day_cnt    ,
      		 klass_sob           ) 
									VALUES        (
			:NEW.models_id 				   ,
      :NEW.datee							 ,
      :NEW.day_num_t 					 , 
			:NEW.year_num_tl	  		 ,   
      :NEW.type_vs_code    		 , 
      :NEW.type_vs_code_ID		 ,
      :NEW.INCIDENT_TYPE			 ,
      :NEW.incident_type_short ,
      :NEW.incident_t_short_ID ,
      :NEW.dubp_code					 ,
      :NEW.short_dubp_code     ,
      :NEW.short_dubt_code_id	 , 
      :NEW.last_adv_day_cnt    ,
      :NEW.klass_sob           );							 
  END IF;   
End;
/


-- Create table
create table RISK_REPORT
(
  models_id          INTEGER not null,
  rep_year           INTEGER not null,
  rep_month          INTEGER not null,  
  short_dubp_code		 VARCHAR2(100) NOT NULL,  
  short_dubt_code_id INTEGER not null,
  type_vs_code			 VARCHAR2(100),
  type_vs_code_id    INTEGER not null,
  exist_in_model     CHAR(1) not null,
  klass_1_rep_value  BINARY_FLOAT default -1,
  klass_2_rep_value  BINARY_FLOAT default -1,
  klass_3_rep_value  BINARY_FLOAT default -1
)
tablespace USERS  ;
-- Add comments to the table 

comment on table  RISK_REPORT  						 IS 'Risk Report Table';
comment on column RISK_REPORT.models_id   			 IS 'модели ID';
comment on column RISK_REPORT.rep_year    			 IS 'year';
comment on column RISK_REPORT.rep_month    			 IS 'month 1 - 12';
comment on column RISK_REPORT.short_dubp_code		 IS 'dubt_code for ease of interactions with existing reports (unnecessarily, but faster)';
comment on column RISK_REPORT.short_dubt_code_id IS 'dubt_code_id_ID';
comment on column RISK_REPORT.type_vs_code    	 IS 'type_vs_code for ease of interactions with existing reports (unnecessarily, but faster)';
comment on column RISK_REPORT.type_vs_code_id    IS 'type_vs_code_ID or 0 if for this code ';
comment on column RISK_REPORT.exist_in_model   	 IS 'Y - There is an entry with the data in model_data_set with the data rep_year, short_dubt_code_id, type_vs_code_ID. N - NO';
comment on column RISK_REPORT.klass_1_rep_value  IS 'Value from C-graph for klass_sob = 1';
comment on column RISK_REPORT.klass_2_rep_value  IS 'Value from C-graph for klass_sob = 2';
comment on column RISK_REPORT.klass_3_rep_value  IS 'Value from C-graph for klass_sob = 3';


-- Create/Recreate primary, unique and foreign key constraints 
alter table RISK_REPORT
   add constraint RISK_REPORT_FK1 foreign key (MODELS_ID)
   references PAWLIN_MODELS (ID) on delete cascade;
  


create table rep_nalet_and_plan
(
  pl_year           NUMBER not null,
  pl_month          NUMBER not null,
  pl_nalet          NUMBER not null,
  data_iu						DATE
) tablespace USERS ;

comment on table  rep_nalet_and_plan  						   IS 'Filled table of actual and planned raids, updated daily';
comment on column rep_nalet_and_plan.pl_year    			   IS 'Year';
comment on column rep_nalet_and_plan.pl_month   			   IS 'Month';
comment on column rep_nalet_and_plan.pl_nalet   			   IS 'Raid Must be updated daily through Job';

ALTER TABLE rep_nalet_and_plan ADD CONSTRAINT rep_nalet_and_plan_pk primary key (pl_year, pl_month);


CREATE OR REPLACE VIEW RISK_REPORT_V AS  
SELECT rr.rep_year, rr.rep_month, pm.name, rr.models_id, rr.short_dubt_code_id, rr.short_dubp_code, rr.type_vs_code, 
   1 risk_po_max_class, 2 risk_po_max_step, 
   sum(rr.klass_1_rep_value) klass_1_rep_value, sum(rr.klass_2_rep_value) klass_2_rep_value, sum(rr.klass_3_rep_value) klass_3_rep_value, 3 nalet
   FROM RISK_REPORT rr, pawlin_models pm 
   WHERE rr.models_id = pm.id
   GROUP BY rr.rep_year, rr.rep_month, pm.name, rr.models_id, rr.short_dubt_code_id, rr.short_dubp_code, rr.type_vs_code
   ORDER BY rr.rep_year, rr.rep_month;
  


CREATE OR REPLACE VIEW V_REP_NALET_AND_PLAN AS
SELECT TO_NUMBER(TO_CHAR(rn.date_start, 'YYYY')) v_year, TO_NUMBER(TO_CHAR(rn.date_start, 'MM')) v_month, SUM(rn.hrs) v_nalet
    FROM v_rep_nalet rn
    WHERE TRUNC(rn.date_start, 'MM') < TRUNC(SYSDATE, 'MM')
    GROUP BY TO_NUMBER(TO_CHAR(rn.date_start, 'YYYY')) , TO_NUMBER(TO_CHAR(rn.date_start, 'MM'))
UNION all
SELECT rn.pl_year v_year, rn.pl_month v_month, rn.pl_nalet v_nalet 
    FROM rep_nalet_and_plan rn
    WHERE rn.pl_year  > TO_NUMBER(TO_CHAR(SYSDATE, 'YYYY')) OR 
         (rn.pl_year  = TO_NUMBER(TO_CHAR(SYSDATE, 'YYYY')) AND rn.pl_month >= TO_NUMBER(TO_CHAR(SYSDATE, 'MM')));




create or replace view v_rep_dubp_risk_model AS
select pm.name, rr.rep_year, rr.rep_month,
    sum(rr.klass_2_rep_value) klass2, 1 k2
   ,sum(rr.klass_3_rep_value) klass3, 10 k3
   ,np.v_nalet VAL
   ,case when rr.rep_year <= EXTRACT( YEAR FROM trunc(sysdate,'MM') )
   AND rr.rep_month < EXTRACT( MONTH FROM trunc(sysdate,'MM') )
       THEN 1 else 0 end HIST
   FROM  RISK_REPORT rr, V_REP_NALET_AND_PLAN np, pawlin_models pm
      where rr.rep_year = np.v_year and rr.rep_month = np.v_month and rr.models_id = pm.id
   GROUp BY pm.name, rep_year, rep_month,np.v_nalet
   ORDER BY rr.rep_year, rr.rep_month;
   
   
   
create table model_k_factor (
  models_id     			INTEGER,
  k_year				      INTEGER DEFAULT 2000,
  incident_t_short_ID INTEGER NOT NULL, 
  klass_sob_k_all     float DEFAULT -1,
  klass_sob_k_1    	 	float DEFAULT -1f,
  klass_sob_k_2    	 	float DEFAULT -1f,
  klass_sob_k_3    	 	float DEFAULT -1f,
  klass_sob_k_4    	 	float DEFAULT -1f,
  klass_sob_k_5    	 	float DEFAULT -1f  				
) tablespace USERS;


COMMENT ON TABLE  model_k_factor 						IS 'Correction factor table';
COMMENT ON COLUMN model_k_factor.klass_sob_k_all   		IS 'Correct K for events of all classes';
COMMENT ON COLUMN model_k_factor.klass_sob_k_1     		IS 'Correct K for 1st class events';
COMMENT ON COLUMN model_k_factor.klass_sob_k_2     		IS 'Correct K for 2nd grade events';
COMMENT ON COLUMN model_k_factor.klass_sob_k_3     		IS 'Correct K for 3rd grade events';
COMMENT ON COLUMN model_k_factor.klass_sob_k_4     		IS 'Correct K for 4rd grade events';
COMMENT ON COLUMN model_k_factor.klass_sob_k_5     		IS 'Correct K for 5rd grade events';

ALTER TABLE model_k_factor ADD CONSTRAINT model_k_factor_pk primary key (models_id, k_year, incident_t_short_ID);
ALTER TABLE model_k_factor ADD CONSTRAINT model_k_factor_fk1 FOREIGN KEY (models_id) 		REFERENCES pawlin_models (id) on delete cascade;