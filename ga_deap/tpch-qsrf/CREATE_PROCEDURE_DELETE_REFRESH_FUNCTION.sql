delimiter //
CREATE PROCEDURE DELETE_REFRESH_FUNCTION ()
	BEGIN
		DELETE lineitem FROM lineitem INNER JOIN rfdelete 
		ON lineitem.l_orderkey = rfdelete.rf_orderkey 
		WHERE lineitem.l_orderkey = rfdelete.rf_orderkey;
	    
		DELETE orders FROM orders INNER JOIN rfdelete 
		ON orders.o_orderkey = rfdelete.rf_orderkey 
		WHERE orders.o_orderkey = rfdelete.rf_orderkey;
	    
		COMMIT;
     END//
delimiter;