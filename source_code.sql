SELECT * FROM pos.trnsact LIMIT 2000

SELECT Count (DISTINCT sku) FROM pos.skuinfo
SELECT Count(DISTINCT store) FROM pos.strinfo
SELECT Count(DISTINCT dept) FROM pos.deptinfo

SELECT * FROM pos.deptinfo
SELECT * FROM pos.strinfo

SELECT sku FROM pos.skuinfo LIMIT 20

SELECT MAX(sku) FROM pos.skuinfo
SELECT MIN(sku) FROM pos.skuinfo

SELECT * FROM pos.skuinfo WHERE sku = 4917933

SELECT * FROM pos.strinfo	

SELECT c1, c4, c7, c8 FROM pos.trnsact WHERE c4 IN 
(SELECT c4 AS trannum FROM pos.trnsact ORDER BY random() LIMIT 10000)

SELECT DISTINCT * FROM
(SELECT c4 AS trannum FROM pos.trnsact ORDER BY random()) AS trannum LIMIT 5804

SELECT c1, c4, c7, c8 FROM pos.trnsact WHERE c4 IN 
(SELECT c4 FROM pos.trnsact WHERE random() <= 0.02)

SELECT * FROM pos.trnsact WHERE c6 = '2005-05-05'

SELECT unique_tran,sku,store,register,trannum,sequence, sold FROM pkk503_schema.transactions og
	INNER JOIN pkk503_schema.all_data s2 ON (og.store = s2.store)
    INNER JOIN pkk503_schema.all_data s3 ON (og.register = s3.register)
    INNER JOIN pkk503_schema.all_data s4 ON (og.trannum = s4.trannum)

SELECT * FROM pos.skuinfo A RIGHT JOIN pkk503_schema.final_skus B ON A.sku = B.sku
	



