

# Database Engine (using Java, B-Tree Data Structure)

Project description

1. Implemented a rudimentary database engine in Java that is based on a hybrid between MySQL and SQLite, operated entirely from the command line.
2. Based on MySQL's InnoDB data engine (SDL), the program uses file-per-table approach to physical storage, implementing Paging using B-Tree (B+Tree) data structure.
3. Commands Supported:
  
  a) DDL- SHOW TABLES, CREATE TABLE, DROP TABLE
  
  b) DML- INSERT INTO, DELETE FROM, UPDATE 
  
  c) VDL- SELECT-FROM-WHERE, EXIT 

---------------------------------------------------------------

How to Run the Project-

1. Open command prompt

2. Change the file directory to the projects file directory

3. Type the command: javac *.java. It will compile all the java files

4. Type the command: java DavisBase. It will run the mail file 

---------------------------------------------------------------

About the Files-

1.MainDavisBase.java - Main file that supports all input commands 

2.DatabaseMethods.java  - contains all methods which are executed with the appropriate input commands

3.BTreeLeafPage.java - conatins pfunctions to implement B+Tree paging

4.CurrentData.java- Stores all information of selected rows/values. 

--------------------------------------------------------------

About the Supported Commands-

1. All the syntax supported by the project can be checked by typing the command "help;".


