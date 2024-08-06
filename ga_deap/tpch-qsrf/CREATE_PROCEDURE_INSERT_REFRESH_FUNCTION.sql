delimiter //
CREATE PROCEDURE INSERT_REFRESH_FUNCTION ()
	BEGIN
		INSERT INTO orders SELECT * FROM orders_temp;
		INSERT INTO lineitem SELECT * FROM lineitem_temp;
     END//
delimiter;