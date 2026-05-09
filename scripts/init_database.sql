/* 
                                  ---------- create a database and schemas ----------
worning: running this will delete any database in your system named 'DataWarehouse' if existed so be carfull.
this script check if there is a database named DataWarehouse and delete it if found and create a new one with the same name, afther htat 
it creates new three schemas named 'bronze' ,'silver' ,'gold'
*/
use master;

go  
if exists (select 1 from sys.databases where name = 'DataWarehouse')
begin 
alter DATABASE DateWarehouse set SINGLE_USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE DataWarehouse ;
end;

go 
create database DataWarehouse ;

go
use DateWarehouse;

go
create schema bronze;

go
create schema silver;

go
create schema gold;
