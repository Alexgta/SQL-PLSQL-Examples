---- 1 

INSERT INTO product_xref VALUES (1001, 'system1', 1, TO_DATE('2015-01-01', 'YYYY-MM-DD'), TO_DATE('2015-01-01', 'YYYY-MM-DD'), 'Title 1');
INSERT INTO product_xref VALUES (1002, 'system2', 2, TO_DATE('2015-01-02', 'YYYY-MM-DD'), TO_DATE('2015-01-02', 'YYYY-MM-DD'), 'Title 1');
INSERT INTO product_xref VALUES (1003, 'system3', 3, TO_DATE('2015-01-03', 'YYYY-MM-DD'), TO_DATE('2015-01-03', 'YYYY-MM-DD'), 'Title 1');
INSERT INTO product_xref VALUES (1004, 'system4', 4, TO_DATE('2015-01-04', 'YYYY-MM-DD'), TO_DATE('2015-01-04', 'YYYY-MM-DD'), 'Title 1');
INSERT INTO product_xref VALUES (1005, 'system5', 5, TO_DATE('2015-05-05', 'YYYY-MM-DD'), TO_DATE('2015-05-05', 'YYYY-MM-DD'), 'Title 1');
INSERT INTO product_xref VALUES (1006, 'system6', 6, TO_DATE('2015-06-06', 'YYYY-MM-DD'), TO_DATE('2015-06-06', 'YYYY-MM-DD'), 'Title 2');
INSERT INTO product_xref VALUES (1007, 'system7', 7, TO_DATE('2015-07-07', 'YYYY-MM-DD'), TO_DATE('2015-07-07', 'YYYY-MM-DD'), 'Title 2');
INSERT INTO product_xref VALUES (1008, 'system8', 8, TO_DATE('2015-08-08', 'YYYY-MM-DD'), TO_DATE('2015-08-08', 'YYYY-MM-DD'), 'Title 2');
INSERT INTO product_xref VALUES (1009, 'system9', 9, TO_DATE('2015-09-09', 'YYYY-MM-DD'), TO_DATE('2015-09-09', 'YYYY-MM-DD'), 'Title 2');
INSERT INTO product_xref VALUES (1010, 'system10',10, TO_DATE('2015-10-10', 'YYYY-MM-DD'), TO_DATE('2015-10-10', 'YYYY-MM-DD'), 'Title 2');


INSERT INTO authors VALUES (1001, 'system1', 'Stephen King', TO_DATE('2015-01-01', 'YYYY-MM-DD'), TO_DATE('2015-01-01', 'YYYY-MM-DD'));
INSERT INTO authors VALUES (1002, 'system2', 'Stephen King', TO_DATE('2015-01-02', 'YYYY-MM-DD'), TO_DATE('2015-01-02', 'YYYY-MM-DD'));
INSERT INTO authors VALUES (1003, 'system3', 'Stephen King', TO_DATE('1999-01-03', 'YYYY-MM-DD'), TO_DATE('2015-01-03', 'YYYY-MM-DD'));
INSERT INTO authors VALUES (1004, 'system4', 'Stephen King', TO_DATE('1999-01-04', 'YYYY-MM-DD'), TO_DATE('1999-11-04', 'YYYY-MM-DD'));

COMMIT;

-- 2

EXECUTE serv_pack.get_all_exceptions('product_xref',
                               'system_name',
                               'authors',
                               'system_name',
                               'ref.id_xref = src.id_xref',
                               'ref.end_date = src.end_date AND ref.end_date = src.out_of_print_date');

SELECT * FROM all_exceptions;

--1	1003	system3       	81	81	    23.03.2018 21:45:20
--2	1004	system4       	81	81, 82	23.03.2018 21:45:20

--------- 

UPDATE authors SET end_date = TO_DATE('2015-01-03', 'YYYY-MM-DD') WHERE id_xref = 1003;
UPDATE authors SET end_date = TO_DATE('2015-01-04', 'YYYY-MM-DD') WHERE id_xref = 1004;
COMMIT;


EXECUTE serv_pack.get_all_exceptions('product_xref',
                               'system_name',
                               'authors',
                               'system_name',
                               'ref.id_xref = src.id_xref',
                               'ref.end_date = src.end_date AND ref.end_date = src.out_of_print_date');

SELECT * FROM all_exceptions;

-- 1	1004	system4       	81	82	23.03.2018 21:45:20


---------

DELETE FROM all_exceptions;
DELETE FROM authors;

INSERT INTO objtree VALUES (1001, 501, 'system1', 'Title-1', TO_DATE('2015-01-01', 'YYYY-MM-DD'), TO_DATE('2015-01-01', 'YYYY-MM-DD'));
INSERT INTO objtree VALUES (1002, 502, 'system2', 'Title-2', TO_DATE('2015-01-02', 'YYYY-MM-DD'), TO_DATE('2015-01-02', 'YYYY-MM-DD'));
INSERT INTO objtree VALUES (1003, 503, 'system99', 'Title-3', TO_DATE('2015-01-03', 'YYYY-MM-DD'), TO_DATE('2015-01-03', 'YYYY-MM-DD'));
INSERT INTO objtree VALUES (1004, 504, 'system99', 'Title-4', TO_DATE('2015-01-04', 'YYYY-MM-DD'), TO_DATE('2222-01-04', 'YYYY-MM-DD'));
COMMIT;

EXECUTE serv_pack.get_all_exceptions('product_xref',
                               'system_name',
                               'authors',
                               'system_name',
                               'ref.id_xref = src.id_xref',
                               'ref.end_date = src.end_date AND ref.end_date = src.out_of_print_date');

SELECT * FROM all_exceptions;

-- 1	1003	2015-01-03	101	101	      23.03.2018 23:19:08
-- 2	1004	2015-01-04	101	102, 101	23.03.2018 23:19:08

----------- 

UPDATE objtree SET system_name = 'system3' WHERE id = 1003;
UPDATE objtree SET system_name = 'system4' WHERE id = 1004;
COMMIT;

EXECUTE serv_pack.get_all_exceptions('product_xref',
                               'system_name',
                               'authors',
                               'system_name',
                               'ref.id_xref = src.id_xref',
                               'ref.end_date = src.end_date AND ref.end_date = src.out_of_print_date');

SELECT * FROM all_exceptions;

--1	1004	2015-01-04	101	102	23.03.2018 23:19:08

------   Repeat with 2 tables: 

DELETE FROM all_exceptions;
DELETE FROM objtree;

INSERT INTO authors VALUES (1001, 'system1', 'Stephen King', TO_DATE('2015-01-01', 'YYYY-MM-DD'), TO_DATE('2015-01-01', 'YYYY-MM-DD'));
INSERT INTO authors VALUES (1002, 'system2', 'Stephen King', TO_DATE('2015-01-02', 'YYYY-MM-DD'), TO_DATE('2015-01-02', 'YYYY-MM-DD'));
INSERT INTO authors VALUES (1003, 'system3', 'Stephen King', TO_DATE('1999-01-03', 'YYYY-MM-DD'), TO_DATE('2015-01-03', 'YYYY-MM-DD'));
INSERT INTO authors VALUES (1004, 'system4', 'Stephen King', TO_DATE('1999-01-04', 'YYYY-MM-DD'), TO_DATE('1999-11-04', 'YYYY-MM-DD'));

INSERT INTO objtree VALUES (1001, 501, 'system1', 'Title-1', TO_DATE('2015-01-01', 'YYYY-MM-DD'), TO_DATE('2015-01-01', 'YYYY-MM-DD'));
INSERT INTO objtree VALUES (1002, 502, 'system2', 'Title-2', TO_DATE('2015-01-02', 'YYYY-MM-DD'), TO_DATE('2015-01-02', 'YYYY-MM-DD'));
INSERT INTO objtree VALUES (1003, 503, 'system99', 'Title-3', TO_DATE('2015-01-03', 'YYYY-MM-DD'), TO_DATE('2015-01-03', 'YYYY-MM-DD'));
INSERT INTO objtree VALUES (1004, 504, 'system99', 'Title-4', TO_DATE('2015-01-04', 'YYYY-MM-DD'), TO_DATE('2222-01-04', 'YYYY-MM-DD'));
COMMIT;



EXECUTE serv_pack.get_all_exceptions('product_xref',
                               'system_name',
                               'authors',
                               'system_name',
                               'ref.id_xref = src.id_xref',
                               'ref.end_date = src.end_date AND ref.end_date = src.out_of_print_date');
                               
EXECUTE serv_pack.get_all_exceptions('product_xref',
                               'system_name',
                               'authors',
                               'system_name',
                               'ref.id_xref = src.id_xref',
                               'ref.end_date = src.end_date AND ref.end_date = src.out_of_print_date');


SELECT * FROM all_exceptions;
--1	1003	system3       	81	81	       23.03.2018 23:33:23
--2	1004	system4       	81	81, 82	   23.03.2018 23:33:23
--3	1003	2015-01-03	    101	101	       23.03.2018 23:31:48
--4	1004	2015-01-04	    101	102, 101   23.03.2018 23:31:48


UPDATE authors SET end_date = TO_DATE('2015-01-03', 'YYYY-MM-DD') WHERE id_xref = 1003;
UPDATE authors SET end_date = TO_DATE('2015-01-04', 'YYYY-MM-DD') WHERE id_xref = 1004;
UPDATE objtree SET system_name = 'system3' WHERE id = 1003;
UPDATE objtree SET system_name = 'system4' WHERE id = 1004;
COMMIT;

-- Now one-time start of the re_check_exceptions procedure without parameters

exec serv_pack.re_check_exceptions;

SELECT * FROM all_exceptions;

--1	1003	system3       	81	OK	    23.03.2018 23:33:23
--2	1004	system4       	81	82	    23.03.2018 23:33:23
--3	1003	2015-01-03	    101	OK	    23.03.2018 23:31:48
--4	1004	2015-01-04	    101	102	    23.03.2018 23:31:48
