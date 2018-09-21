DROP PROCEDURE clearhlong;
delimiter $$
CREATE PROCEDURE clearhlong()
BEGIN
  SET @hlong_count = 0;
  select count(1) INTO @hlong_count from information_schema.tables WHERE table_schema = 'best_stocks' AND table_name LIKE '%hlong%';
  WHILE @hlong_count > 0 DO
    SET @tables = NULL;
    SELECT GROUP_CONCAT(table_schema, '.`', table_name, '`') INTO @tables FROM
    (select * from
     information_schema.tables
      WHERE table_schema = 'best_stocks' AND table_name LIKE '%hlong%'
      LIMIT 10) TT;

    SET @tables = CONCAT('DROP TABLE ', @tables);
    select @tables;
    PREPARE stmt1 FROM @tables;
    EXECUTE stmt1;
    DEALLOCATE PREPARE stmt1;
  END WHILE;
END;
$$
delimiter ;
call clearhlong();
