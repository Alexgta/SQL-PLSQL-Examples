CREATE TABLE "ORDMMAPP"."TEST_EMPLOY"
(EMPLOY_ID NUMBER,
 FIRST_NAME VARCHAR2(100),
 LAST_NAME VARCHAR2(100),
 SALARY NUMBER
);

Insert into ORDMMAPP.TEST_EMPLOY (EMPLOY_ID,FIRST_NAME,LAST_NAME,SALARY) values (1,'Jhon','Smith',100);
Insert into ORDMMAPP.TEST_EMPLOY (EMPLOY_ID,FIRST_NAME,LAST_NAME,SALARY) values (2,'Igor','Doe',200);
Insert into ORDMMAPP.TEST_EMPLOY (EMPLOY_ID,FIRST_NAME,LAST_NAME,SALARY) values (3,'Peter ','Smith',300);
Insert into ORDMMAPP.TEST_EMPLOY (EMPLOY_ID,FIRST_NAME,LAST_NAME,SALARY) values (4,'Scott','Lee',400);
Insert into ORDMMAPP.TEST_EMPLOY (EMPLOY_ID,FIRST_NAME,LAST_NAME,SALARY) values (5,'Jones','Ivanov',500);


create or replace function test_records_of_tables (p_first_name varchar2 default null, p_last_name varchar2 default null)
  RETURN NUMBER
AS
  TYPE employ_rec IS RECORD (
  employ_id NUMBER,
  first_name VARCHAR2(100),
  last_name VARCHAR2(100),
  salary NUMBER
  );

  TYPE employ_tbl_type IS TABLE OF employ_rec INDEX BY VARCHAR2(200);

  employ_tbl employ_tbl_type;

  CURSOR cur_employ IS
  SELECT t.employ_id, t.first_name, t.last_name, t.salary
  FROM test_employ t;
  v_key1  varchar2(200);
  v_result NUMBER := 0;
BEGIN
  v_result := -1;

  FOR rc IN cur_employ LOOP
    v_key1 := rc.first_name || rc.last_name;

    employ_tbl(v_key1).employ_id := rc.employ_id;
    employ_tbl(v_key1).first_name := rc.first_name;
    employ_tbl(v_key1).last_name := rc.last_name;
    employ_tbl(v_key1).salary := rc.salary;

  END LOOP;

  BEGIN
    v_result := employ_tbl(p_first_name || p_last_name).salary;
  EXCEPTION
  WHEN OTHERS THEN
    v_result := -1;
  END;

  RETURN v_result;

END test_records_of_tables;
