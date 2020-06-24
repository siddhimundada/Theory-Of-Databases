 CREATE PROCEDURE stddev(OUT std DECIMAL(30,10))
 	LANGUAGE SQL
 	BEGIN
 		DECLARE SQLSTATE CHAR(5) DEFAULT '00000';
 		DECLARE sum DECIMAL(30,10);
 		DECLARE sum1 DECIMAL(30,10);
 		DECLARE sal DECIMAL(9,2);
 		DECLARE mean1 DECIMAL(30,10);
 		DECLARE mean2 DECIMAL(30,10);
 		DECLARE var DECIMAL(30,10);


 		DECLARE count INTEGER;
 		DECLARE c CURSOR FOR SELECT SALARY FROM EMPLOYEE;

 			SET sum=0.0;
 			SET sum1=0.0;
 			SET count=0;
 			OPEN c;
 			FETCH FROM c into sal;
 			WHILE (SQLSTATE ='00000')DO
 				SET sum=sum+sal;
 				SET sum1=sum1+sal*sal; 				
 				SET count=count+1;
 				FETCH FROM c INTO sal;
 			END WHILE;
 			CLOSE c;
 			SET count =count;
 			SET mean1=sum/count;
 			SET mean2=sum1/(COUNT);
 			SET var=mean2 - (mean1*mean1);
 			SET std=SQRT(var);

 	END
 	@