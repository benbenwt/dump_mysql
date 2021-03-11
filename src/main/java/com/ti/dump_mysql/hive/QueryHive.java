package com.ti.dump_mysql.hive;

import com.ti.dump_mysql.utils.MysqlConnect;


import java.sql.*;
import java.text.ParseException;


public class QueryHive {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    Connection conn;
    MysqlConnect mysqlConnect;
    public void init(String url,String user,String password) throws ClassNotFoundException, SQLException {

        Class.forName(driverName);
        conn= DriverManager.getConnection(url,user,password);
        System.out.println(conn);

        mysqlConnect=new MysqlConnect();
        String mysqlUrl="jdbc:mysql://hbase2:3306/platform?useUnicode=true&characterEncoding=utf-8&useSSL=true&serverTimezone=UTC";
        mysqlConnect.init(mysqlUrl,"root","root");
    }

    public void dumpCategory() throws SQLException, ClassNotFoundException, ParseException {
        /*count,time,category*/
        String sql=" select type,sampletime, count(*) nums   from sample where type!='NULL' and sampletime!='NULL' group by type,sampletime";
        Statement st=conn.createStatement();
        ResultSet resultSet=st.executeQuery(sql);
        System.out.println("category :");
        mysqlConnect.insertCategory(resultSet);
    }
    public void dumpArch() throws SQLException, ClassNotFoundException {
        String sql="select architecture,count(1) nums from sample where architecture!='NULL' group by architecture";
        Statement st=conn.createStatement();
        ResultSet resultset=st.executeQuery(sql);
        System.out.println("arch: " );
        mysqlConnect.insertArch(resultset);
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException, ParseException {
        QueryHive queryHive=new QueryHive();
        queryHive.init("jdbc:hive2://hbase:10000/platform","root","root");

        queryHive.dumpArch();
        queryHive.dumpCategory();
    }

}
