CREATE TABLE product_xref (
       id_xref     NUMBER(38,0) NOT NULL ENABLE, 
       system_name CHAR(14 CHAR) NOT NULL ENABLE, 
       obj_id      NUMBER(38,0), 
       end_date    DATE, 
       out_of_print_date DATE, 
       title      VARCHAR2(1000 CHAR)
) tablespace user_data;


COMMENT ON TABLE product_xref IS 'Reference Table';
ALTER TABLE product_xref ADD CONSTRAINT product_xref_pk PRIMARY KEY (id_xref) USING INDEX TABLESPACE USER_DATA;


CREATE TABLE authors (
      id_xref      NUMBER(38,0) NOT NULL ENABLE,
      system_name  CHAR(14 CHAR) NOT NULL ENABLE, 
      author       CHAR(100 CHAR) NOT NULL ENABLE,
      end_date     DATE, 
      out_of_print_date DATE
) tablespace user_data;

COMMENT ON TABLE authors IS 'Authors input table';
ALTER TABLE authors ADD CONSTRAINT authors_id_xref_pk PRIMARY KEY (id_xref) USING INDEX TABLESPACE USER_DATA;

DELETE FROM authors;

CREATE TABLE all_errors (
    err_id             NUMBER NOT NULL ENABLE,
    ref_table_name     VARCHAR2(100 CHAR), 
    ref_column_name    VARCHAR2(100 CHAR),
    src_table_name     VARCHAR2(100 CHAR),
    src_column_name    VARCHAR2(100 CHAR),
    join_condition     VARCHAR2(500 CHAR),
    compare_condition  VARCHAR2(500 CHAR)
) tablespace user_data;

ALTER TABLE all_errors ADD CONSTRAINT all_errors_pk PRIMARY KEY (err_id) USING INDEX TABLESPACE USER_DATA;

COMMENT ON TABLE  all_errors IS 'Errors table';
COMMENT ON COLUMN all_errors.ref_table_name    IS 'Reference Table Name';
COMMENT ON COLUMN all_errors.ref_column_name   IS 'Reference Table Field';
COMMENT ON COLUMN all_errors.src_table_name    IS 'Input table name';
COMMENT ON COLUMN all_errors.src_column_name   IS 'Input table field';
COMMENT ON COLUMN all_errors.join_condition    IS 'Join Constraint';
COMMENT ON COLUMN all_errors.compare_condition IS 'Comparative Constraint defining a pattern match';

create sequence all_errors_sq1 start with 1 increment by 1;

CREATE OR REPLACE TRIGGER all_errors_bi  BEFORE INSERT ON all_errors
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
Begin
  IF :NEW.err_id IS NULL THEN 
    SELECT all_errors_sq1.nextval INTO :NEW.err_id FROM SYS.DUAL;
  END IF;   
End;


CREATE TABLE all_errors_details (
    id               NUMBER NOT NULL ENABLE,
    err_id           NUMBER NOT NULL ENABLE,    
    error_condition  VARCHAR2(500 CHAR)
) tablespace user_data;

COMMENT ON TABLE  all_errors_details IS 'Mistakes detail error table';
COMMENT ON COLUMN all_errors_details.error_condition  IS 'Broken condition';
ALTER TABLE all_errors_details ADD CONSTRAINT all_errors_details_fk1 FOREIGN KEY (err_id) REFERENCES all_errors (err_id) on delete cascade;


create sequence all_errors_details_bi start with 1 increment by 1;
CREATE OR REPLACE TRIGGER all_errors_details_bi  BEFORE INSERT ON all_errors_details
REFERENCING NEW AS NEW OLD AS OLD
FOR EACH ROW
Begin
  IF :NEW.id IS NULL THEN 
    SELECT all_errors_details_bi.nextval INTO :NEW.id FROM SYS.DUAL;
  END IF;   
End;


CREATE TABLE all_exceptions (
    id_xref          NUMBER(38,0) NOT NULL ENABLE,        
    --XREF_TABLE_NAME VARCHAR2(30 CHAR)              --- I threw out this field, because I think that it is not necessary here. 
    src_key_val      VARCHAR2(255),  
    err_id           NUMBER NOT NULL ENABLE,         --- this field is not necessary, but it is more comfortable to leave it
    err_details_ids  VARCHAR2(100),
    create_date      DATE   
) tablespace user_data;

ALTER TABLE all_exceptions ADD CONSTRAINT all_exceptions_fk1 FOREIGN KEY (err_id) REFERENCES all_errors (err_id) on delete cascade;

COMMENT ON TABLE  all_exceptions IS 'Table with all exceptions';
COMMENT ON COLUMN all_exceptions.id_xref  IS 'PK from ref table';
COMMENT ON COLUMN all_exceptions.src_key_val  IS 'value from input table';
COMMENT ON COLUMN all_exceptions.err_id  IS 'Code line selection condition';
COMMENT ON COLUMN all_exceptions.err_details_ids  IS 'A descriptive field collected from the mismatch codes in the all_errors_details table';



---------------  second table for package testing. 

CREATE TABLE objtree (
      id           NUMBER(38,0) NOT NULL ENABLE,
      obj_id       NUMBER(38,0),
      system_name  CHAR(14 CHAR) NOT NULL ENABLE, 
      title        VARCHAR2(1000 CHAR), 
      end_date     DATE, 
      out_of_print_date DATE
) tablespace user_data;

COMMENT ON TABLE objtree IS 'Second input table';
ALTER TABLE objtree ADD CONSTRAINT objtree_id_pk PRIMARY KEY (id) USING INDEX TABLESPACE USER_DATA;

