package com.hibernate;


import java.util.Collections;
import java.util.Comparator;
import java.util.List;  

import org.hibernate.Criteria;
import org.hibernate.HibernateException; 
import org.hibernate.Query;
import org.hibernate.Session; 
import org.hibernate.Transaction;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;

import com.hibernate.employeebean.Employee;

public class HibernateTest {
	private static SessionFactory factory; 
	@SuppressWarnings({ "deprecation", "unused" })
	public static void main(String[] args) {
		try{
			factory = new Configuration().configure().buildSessionFactory();
		}catch (Throwable ex) { 
			System.err.println("Failed to create sessionFactory object." + ex);
			throw new ExceptionInInitializerError(ex); 
		}
		HibernateTest ME = new HibernateTest();

		/* Truncate Table to clear the past data */
		ME.truncateEmployeeTable();

		/* Add few employee records in database */
		Integer empID1 = ME.addEmployee("Zara", "Ali", 1000);
		Integer empID2 = ME.addEmployee("Daisy", "Das", 5000);

		Integer empID3 = ME.addEmployee("John", "Paul", 10000);
		Integer empID4 = ME.addEmployee("Siddartha", "Konka", 9000);
		Integer empID5 = ME.addEmployee("Ramnath", "M", 50000);
		Integer empID6 = ME.addEmployee("Phani", "Repala", 10500);
		Integer empID7 = ME.addEmployee("Sriraj", "Vasireddy", 18000);
		Integer empID8 = ME.addEmployee("Kousalya", "Kotagiri", 100000);

		/* List down all the employees */
		ME.listEmployees();

		/* Update employee's records */
		ME.updateEmployee(empID1, "Srujan","G",5000);

		/* Delete an employee from the database */
		ME.deleteEmployee(empID2);

		/* List down new list of the employees */
		ME.listEmployees();

		/*Search Employee with first name Srujan*/
		String fname="Srujan";
		ME.searchEmployee(fname);

		/*Count number of Employees*/
		ME.countEmployee();

		/*Sum of Salaries */
		ME.totalSalary();

		/*Find the name like the given String*/
		String likeString="sru";
		ME.likeNameCompare(likeString);

		/*Get salary of Given employee*/
		ME.findEmployeeSalary("srujan");

	}

	/*Method to TRUNCATE the employee table*/
	public void truncateEmployeeTable(){
		Session s=factory.openSession();
		Transaction tx=null;
		int rowseffected=0;
		try{
			tx=s.beginTransaction();
			String hql="TRUNCATE TABLE Employee";
			Query q=s.createSQLQuery(hql);
			rowseffected=q.executeUpdate();
			System.out.println("Rows Effected: "+rowseffected);
			tx.commit();	
		}catch(Exception ex){
			if(tx!=null)
				ex.printStackTrace();
		}finally{
			s.close();
		}
	}

	/* Method to CREATE an employee in the database */
	public Integer addEmployee(String fname, String lname, int salary){
		Session s = factory.openSession();
		Transaction tx=null;
		Integer EmployeeId=null;
		try{
			tx=s.beginTransaction();
			Employee e=new Employee(fname,lname,salary);
			EmployeeId =(Integer) s.save(e);
			tx.commit();

		}catch(Exception ex){
			if(tx!=null){
				tx.rollback();
				ex.printStackTrace();
			}
		}finally{
			s.close();
		}
		return EmployeeId;
	}

	/* Method to  READ all the employees */
	@SuppressWarnings("unchecked")
	public void listEmployees( ){
		Session session = factory.openSession();
		Transaction tx = null;
		try{
			tx = session.beginTransaction();
			List<Employee> employees = session.createQuery("FROM Employee").list(); 
			Collections.sort(employees,new Comparator<Employee>(){

				//Sorted Employee list based on their salary in Descending order
				@Override
				public int compare(Employee e1, Employee e2) {
					Integer s1=e1.getSalary();
					Integer s2=e2.getSalary();
					return -s1.compareTo(s2);	
				}

			});
			for (Employee employee : employees){ 
				System.out.print("First Name: " + employee.getFirstName()); 
				System.out.print("  Last Name: " + employee.getLastName()); 
				System.out.println("  Salary: " + employee.getSalary()); 
			}
			tx.commit();
		}catch (HibernateException e) {
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		}finally {
			session.close(); 
		}
	}
	/* Method to UPDATE salary for an employee */
	public void updateEmployee(Integer EmployeeID, String fname, String lname, int salary ){
		Session session = factory.openSession();
		Transaction tx = null;
		try{
			tx = session.beginTransaction();
			Employee employee = 
					(Employee)session.get(Employee.class, EmployeeID);
			employee.setFirstName(fname);
			employee.setLastName(lname);
			employee.setSalary( salary );
			session.update(employee); 
			tx.commit();
		}catch (HibernateException e) {
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		}finally {
			session.close(); 
		}
	}
	/* Method to DELETE an employee from the records */
	public void deleteEmployee(Integer EmployeeID){
		Session session = factory.openSession();
		Transaction tx = null;
		try{
			tx = session.beginTransaction();
			Employee employee = 
					(Employee)session.get(Employee.class, EmployeeID); 
			session.delete(employee); 
			tx.commit();
		}catch (HibernateException e) {
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		}finally {
			session.close(); 
		}
	}
	/*Method to Search an Employee by FirstName*/
	@SuppressWarnings("unchecked")
	public void searchEmployee(String fname){
		Session s=factory.openSession();
		Transaction tx=null;
		try{
			tx=s.beginTransaction();
			//Search using HQL 
			// Query query = s.createQuery("from Employee where firstName = :name ");
			// query.setParameter("name", fname);

			//Search using Hibernate Criteria Query
			Criteria cr=s.createCriteria(Employee.class);
			cr.add(Restrictions.eq("firstName", fname));
			List<Employee> employeeList= cr.list();
			for(Employee e: employeeList){
				System.out.println("Name:"+e.getFirstName()+","+e.getLastName());
			}
			// Iterator<?> iterator = employeeList.iterator();
			// while(iterator.hasNext()){
			//     Employee employee = (Employee) iterator.next(); 
			//    System.out.println("Name:"+employee.getFirstName()+","+employee.getLastName());
			// }
			tx.commit();
		}catch(Exception e){
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		}finally{
			s.close();
		}

	}

	/* Method to print total number of records */
	public void countEmployee(){
		Session session = factory.openSession();
		Transaction tx = null;
		try{
			tx = session.beginTransaction();
			Criteria cr = session.createCriteria(Employee.class);

			// To get total row count.
			cr.setProjection(Projections.rowCount());
			List<?> rowCount = cr.list();

			System.out.println("Total Count: " + rowCount.get(0) );
			tx.commit();
		}catch (HibernateException e) {
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		}finally {
			session.close(); 
		}
	}

	/* Method to print sum of salaries */
	public void totalSalary(){
		Session session = factory.openSession();
		Transaction tx = null;
		try{
			tx = session.beginTransaction();
			Criteria cr = session.createCriteria(Employee.class);

			// To get total salary.
			cr.setProjection(Projections.sum("salary"));
			List<?> totalSalary = cr.list();

			System.out.println("Total Salary: " + totalSalary.get(0) );
			tx.commit();
		}catch (HibernateException e) {
			if (tx!=null) tx.rollback();
			e.printStackTrace(); 
		}finally {
			session.close(); 
		}
	}

	//search employee by name

	@SuppressWarnings("unchecked")
	public void likeNameCompare(String name){
		Session session=factory.openSession();
		Transaction transaction=null;
		try{
			transaction=session.beginTransaction();
			Criteria criteria=session.createCriteria(Employee.class);
			criteria.add(Restrictions.like("firstName", name+"%"));

			List<Employee> employees=criteria.list();
			System.out.println("The Names are:");
			for(Employee e:employees){
				System.out.println(e.toString());
			}
		}catch(Exception ex){
			if(transaction!=null){
				transaction.rollback();
			}
			ex.printStackTrace();


		}finally{
			session.close();
		}
	}

	@SuppressWarnings("unchecked")
	public void findEmployeeSalary(String name){
		Session s=factory.openSession();
		Transaction tx=null;
		try{
			tx=s.beginTransaction();

			Criteria cr=s.createCriteria(Employee.class);
			cr.add(Restrictions.ilike("firstName", name));
			List<Employee> list=cr.list();

			//			String hql="SELECT E.salary FROM Employee E WHERE firstName= :name";
			//			Query q=s.createQuery(hql);
			//			q.setParameter("name", name);
			//			List<Employee> list=q.list();
			System.out.print("The salary of "+name+"is: "+list.get(0));
			tx.commit();

		}catch(Exception ex){
			if(tx!=null)
				ex.printStackTrace();
		}finally{
			s.close();
		}
	}
}