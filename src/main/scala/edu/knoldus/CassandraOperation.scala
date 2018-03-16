package edu.knoldus


import com.datastax.driver.core.{ConsistencyLevel, Session}

import scala.collection.JavaConverters._


object CassandraOperation extends App with CassandraProviders {


    cassandraSession.getCluster.getConfiguration.getQueryOptions.setConsistencyLevel(ConsistencyLevel.QUORUM)

  println("\n\n*********Retrieve Employee Data Example *************")
  getUsersAllDetails(cassandraSession)
  deleteUserLivingInChandigarh(cassandraSession)
  cassandraSession.close()


  private def getUsersAllDetails(inSession: Session): Unit = {
    //Creation of table

    inSession.execute(s"CREATE TABLE IF NOT EXISTS employee(emp_id int, emp_name text,emp_city text,emp_salary varint,emp_phone varint,primary key(emp_id,emp_salary));")
    //Inserting employee details

    inSession.execute(s"insert into employee(emp_id, emp_name, emp_city, emp_salary, emp_phone) values(1,'Bhawna','Delhi',50000,9876543210);")
    inSession.execute(s"insert into employee(emp_id, emp_name, emp_city, emp_salary, emp_phone) values(2,'Gaurav','Noida',100000,9898993210);")
    inSession.execute(s"insert into employee(emp_id, emp_name, emp_city, emp_salary, emp_phone) values(3,'Neel','New-Delhi',7000,9876543210);")
    inSession.execute(s"insert into employee(emp_id, emp_name, emp_city, emp_salary, emp_phone) values(4,'Parul','Noida',60000,8875543210);")

    //Fetch the data for a particular emp_id.
    val result1 = inSession.execute(s"SELECT * FROM employee where emp_id = 3").asScala.toList
    result1.foreach(println(_))
    //Update the record  with id as 1 and salary as 50000 to have the emp_city as chandigarh.
    inSession.execute(s"update employee set emp_city = 'Chandigarh' where emp_id = 1 and emp_salary = 50000;")
    val result2 = inSession.execute(s"select * from employee;").asScala.toList
    result2.foreach(println(_))

    //Fetch the data for the records having salary > 30000 and id = 1
    val result3 = inSession.execute(s"select * from employee where emp_id = 3 and emp_salary > 3000;").asScala.toList
    result3.foreach(println(_))
    //Fetch the details of all the employees who live in chandigarh
    inSession.execute(s"create index if not exists cityIndex on employee(emp_city);")
    val result4 = inSession.execute(s"select * from employee where emp_city = 'Chandigarh';").asScala.toList
    result4.foreach(println(_))
  }


  private def deleteUserLivingInChandigarh(inSession: Session): Unit = {

    inSession.execute("create table emprecord(emp_id int,emp_name text,emp_city text,emp_salary varint,emp_phone varint,primary key(emp_city));")
    inSession.execute("insert into emprecord(emp_id, emp_name, emp_city, emp_salary, emp_phone) values(1,'Bhawna','Chandigarh',50000,9876543210);")
    inSession.execute("insert into emprecord(emp_id, emp_name, emp_city, emp_salary, emp_phone) values(2,'Gaurav','Noida',100000,9898993210);")
    inSession.execute("delete from emprecord where emp_city = 'Chandigarh;")
    println("deletion done")
    val result5 = inSession.execute("select * from emprecord;").asScala.toList
    result5.foreach(println(_))
  }
}