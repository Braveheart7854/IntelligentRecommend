/**
 * Created by tongh on 2018/10/11.
 */
import groovy.sql.DataSet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

/**
 * Created by Administrator on 2017/11/6.
 */
public class SparkMysql implements Serializable{
    public static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(SparkMysql.class);

//    public static void main(String[] args) {
//        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("SparkMysql").setMaster("local[5]"));
//        SQLContext sqlContext = new SQLContext(sparkContext);
//        //读取mysql数据
//        readMySQL(sqlContext);
//
//        //停止SparkContext
//        sparkContext.stop();
//    }
    public JavaRDD<String[]> readMySQL(SQLContext sqlContext){
//    public List<Row> readMySQL(SQLContext sqlContext){
        //jdbc.url=jdbc:mysql://localhost:3306/database
        String url = "jdbc:mysql://localhost:3306/test";
        //查找的表名
        String table = "ratings";
        //增加数据库的用户名(user)密码(password),指定test数据库的驱动(driver)
        Properties connectionProperties = new Properties();
        connectionProperties.put("user","root");
        connectionProperties.put("password","hello123");
        connectionProperties.put("driver","com.mysql.jdbc.Driver");

        //SparkJdbc读取Postgresql的products表内容
//        System.out.println("读取test数据库中的movies表内容");
        // 读取表中所有数据
        DataFrame jdbcDF = sqlContext.read().jdbc(url,table,connectionProperties).select("*");//.where("id=1");
        //显示数据
//        jdbcDF.show();


        JavaRDD<Row> res = jdbcDF.javaRDD();
        JavaRDD<String[]> stringJavaRDD = res.map(new Function<Row, String[]>() {
            @Override
            public String[] call(Row row) throws Exception {
                String[] st = new String[4];
                st[0] = String.valueOf(row.getInt(1));
                st[1] = String.valueOf(row.getInt(2));
                st[2] = String.valueOf(row.getDouble(3));
                st[3] = String.valueOf(row.getInt(4));
                return st;
            }
        });

//        List<Row> res2 = jdbcDF.javaRDD().collect();
//
//        for(Row row : res2){
//            System.out.println(row);
//        }
        return stringJavaRDD;

    }
}
