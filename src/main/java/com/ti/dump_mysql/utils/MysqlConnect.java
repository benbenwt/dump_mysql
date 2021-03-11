package com.ti.dump_mysql.utils;



import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


public class MysqlConnect {
    public static final  String driverName="com.mysql.cj.jdbc.Driver";
    Connection conn;
    public void init(String url,String user,String root) throws ClassNotFoundException, SQLException {
        Class.forName(driverName);
        conn= DriverManager.getConnection(url,user,root);
        System.out.println(conn);
        Statement st=conn.createStatement();
        st.execute("delete from category_tbl");
        st.execute("delete from  architecture");
    }
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Map<String, Map<String,Integer>> info=new HashMap<>();
        System.out.println(info.get("a"));
//        MysqlConnect mysqlConnect=new MysqlConnect();
//        String url="jdbc:mysql://localhost:3306/platform?useUnicode=true&characterEncoding=utf-8&useSSL=true&serverTimezone=UTC";
//        mysqlConnect.init(url,"root","root");
//        mysqlConnect.insertSample();
    }
    public Map<String, Map<String,Integer>>  caculateAccuNums(ResultSet resultSet) throws SQLException {
        /*三维数组吧，才能随机读取.category,time,value*/
        Map<String, Map<String,Integer>> info=new HashMap<>();
        Map<String,Integer> tmp=new HashMap<>();
        Map<String,Integer> categoryCount=new HashMap<>();
        while(resultSet.next())
        {
            String category=resultSet.getString(1);
            String time=resultSet.getString(2);
            Integer value=resultSet.getInt(3);

            String subTime=time.substring(0,10);

            tmp.clear();
            tmp.put(category,value);

            info.put(subTime,new HashMap<>(tmp));

            categoryCount.put(category,0);
        }
        System.out.println(categoryCount);

        LocalDate localDate=LocalDate.now();
        LocalDate startDate=localDate.minusDays(360);
        LocalDate vardate=startDate;
        LocalDate endBorder=localDate.plusDays(1);

        Map<String, Map<String,Integer>> result=new HashMap<>();
        Map<String, Integer> resultTmp=new HashMap<>();
        Map<String, Integer> categoryValue;
        int newCount;
        //
        while(vardate.isBefore(endBorder))
        {//循环日期
            System.out.println("vardate："+vardate);

            categoryValue= info.get(vardate.toString());
            System.out.println("add ："+categoryValue);

            if(categoryValue!=null)
            {//遍历不为0类别
                //count
                for(Map.Entry<String,Integer> entry:categoryValue.entrySet())
                {
                    newCount=categoryCount.get(entry.getKey())+entry.getValue();
                    categoryCount.put(entry.getKey(),newCount);
                }
            }
            //当前vardate，累计categoryCount数目存储
            for(Map.Entry<String,Integer> entry:categoryCount.entrySet())
            {
                resultTmp.put(entry.getKey(),categoryCount.get(entry.getKey()));
                result.put(vardate.toString(),resultTmp);
            }
            System.out.println("accu :"+categoryCount);
            vardate=vardate.plusDays(1);
        }
        return result;
    }
    public void  insertCategory(ResultSet resultSet) throws SQLException, ParseException {
        Map<String, Map<String,Integer>> result=caculateAccuNums(resultSet);

        PreparedStatement ps=conn.prepareStatement("insert into category_tbl(time,category,`value`) values(?,?,?)");
        /*time category nums*/
        for(Map.Entry<String,Map<String,Integer>> entry:result.entrySet())
        {
            //time
            SimpleDateFormat simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd");
            Date date=simpleDateFormat.parse(entry.getKey());
            java.sql.Date sqlDate=new java.sql.Date(date.getTime());
            ps.setDate(1,sqlDate);

           for(Map.Entry<String,Integer> entry1:entry.getValue().entrySet())
           {
               String categoryStr=entry1.getKey();
               ps.setString(2,categoryStr);

               int nums=entry1.getValue();
               ps.setInt(3,nums);
               ps.execute();
           }
        }
        ps.close();
    }

    public void insertArch(ResultSet arch) throws SQLException {
        System.out.println("insertArch");
        Statement st=conn.createStatement();
        while(arch.next())
        {
            String sql="insert into architecture(architecture,`value`) values('"+arch.getString(1)+"',"+arch.getString(2)+");";
            System.out.println(sql);
            st.execute(sql);
        }
        st.close();
    }

}
