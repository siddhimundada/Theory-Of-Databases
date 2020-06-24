import java.sql.*;
import java.io.*;
import java.lang.*;

class SalaryStdDev{
	
	public static void main(String args[]){
		try{
			Class.forName("com.ibm.db2.jcc.DB2Driver");
			Connection con=null;
			String url="jdbc:db2://localhost:50000/"+args[0];
			
			String userid = args[2];
			String passwd = args[3];
			con= DriverManager.getConnection(url,userid,passwd);


			Statement stmt = con.createStatement();
			String q="Select Salary from " + args[1];
			ResultSet rs=stmt.executeQuery(q);
			double sum=0.0;
			double sum1=0.0;
			int count=0;
			while(rs.next()){
				sum=sum+rs.getDouble(1);
				sum1=sum1+Math.pow(rs.getDouble(1),2);
				count=count+1;
			}

			double mean=sum/count;
			double mean2=sum1/count;
			double var=mean2 - (mean*mean);
			double std = Math.sqrt(var);
			System.out.println("Standard deviation: "+ std);
			rs.close();
			stmt.close();
			con.close();
		}
		catch(Exception e){
			e.printStackTrace();
	}

	}
	

	}

