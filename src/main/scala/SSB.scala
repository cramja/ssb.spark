/* SSB.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._
import java.io.File

case class LineOrder (
 lo_orderkey : Int,
 lo_linenumber : Int,
 lo_custkey : Int,
 lo_partkey : Int,
 lo_suppkey : Int,
 lo_orderdate : Int,
 lo_orderpriority : String,
 lo_shippriority : String,
 lo_quantity : Int,
 lo_extendedprice : Int,
 lo_ordtotalprice : Int,
 lo_discount : Int,
 lo_revenue : Int,
 lo_supplycost : Int,
 lo_tax : Int,
 lo_commitdate : Int,
 lo_shipmode : String
 )

case class Part (
 p_partkey : Int,
 p_name : String,
 p_mfgr : String,
 p_category : String,
 p_brand1 : String,
 p_color : String,
 p_type : String,
 p_size : Int,
 p_container : String
 )

case class Supplier (
 s_suppkey : Int,
 s_name : String,
 s_address : String,
 s_city : String,
 s_nation : String,
 s_region : String,
 s_phone : String
 )

case class Customer (
 c_custkey : Int,
 c_name : String,
 c_address : String,
 c_city : String,
 c_nation : String,
 c_region : String,
 c_phone : String,
 c_mktsegment : String
 )

case class Ddate (
 d_datekey : Int,
 d_date : String,
 d_dayofweek : String,
 d_month : String,
 d_year : Int,
 d_yearmonthnum : Int,
 d_yearmonth : String,
 d_daynuminweek : Int,
 d_daynuminmonth : Int,
 d_daynuminyear : Int,
 d_monthnuminyear : Int,
 d_weeknuminyear : Int,
 d_sellingseason : String,
 d_lastdayinweekfl : Int,
 d_lastdayinmonthfl : Int,
 d_holidayfl : Int,
 d_weekdayfl : Int
 )

object SSB {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SSB")
    val sc = new SparkContext(conf)
    val sqlContext : org.apache.spark.sql.SQLContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    var table_names = Array("lineorder", "part", "supplier", "customer", "date")
    // for (i <- 0 until table_names.length) {
    //     val lineorder =  sc.textFile(f.getPath()).map(_.split("\\|")).map(p => LineOrder(p(0).trim.toInt, p(1).trim.toInt,p(2).trim.toInt,p(3).trim.toInt,p(4).trim.toInt,p(5).trim.toInt,p(6),p(7),p(8).trim.toInt,p(9).trim.toInt,p(10).trim.toInt,p(11).trim.toInt,p(12).trim.toInt,p(13).trim.toInt,p(14).trim.toInt,p(15).trim.toInt,p(16))).toDF()
    // }

    val ssb_path = "/home/marc/code/benchmarks/tpch+ssb/ssb_tbl/sf1/"
    val f: java.io.File = new File(ssb_path ,"lineorder.tbl")
    val lineorder =  sc.textFile(f.getPath()).map(_.split("\\|")).map(p => LineOrder(p(0).trim.toInt, p(1).trim.toInt,p(2).trim.toInt,p(3).trim.toInt,p(4).trim.toInt,p(5).trim.toInt,p(6),p(7),p(8).trim.toInt,p(9).trim.toInt,p(10).trim.toInt,p(11).trim.toInt,p(12).trim.toInt,p(13).trim.toInt,p(14).trim.toInt,p(15).trim.toInt,p(16))).toDF()
    val customer = sc.textFile((new File(ssb_path, "customer.tbl")).getPath()).map(_.split("\\|")).map(p => Customer(p(0).trim.toInt, p(1),p(2),p(3),p(4),p(5),p(6),p(7))).toDF()

    val supplier = sc.textFile((new File(ssb_path,"supplier.tbl")).getPath()).map(_.split("\\|")).map(p => Supplier(p(0).trim.toInt, p(1),p(2),p(3),p(4),p(5),p(6))).toDF()

    val date = sc.textFile((new File(ssb_path,"date.tbl")).getPath()).map(_.split("\\|")).map(p => Ddate(p(0).trim.toInt, p(1),p(2),p(3),p(4).trim.toInt,p(5).trim.toInt,p(6),p(7).trim.toInt,p(8).trim.toInt,p(9).trim.toInt,p(10).trim.toInt,p(11).trim.toInt,p(12),p(13).trim.toInt,p(14).trim.toInt,p(15).trim.toInt,p(16).trim.toInt)).toDF()

    val part = sc.textFile((new File(ssb_path,"part.tbl")).getPath()).map(_.split("\\|")).map(p => Part(p(0).trim.toInt, p(1),p(2),p(3),p(4),p(5),p(6),p(7).trim.toInt,p(8))).toDF()

    lineorder.registerTempTable("lineorder")
    customer.registerTempTable("customer")
    supplier.registerTempTable("supplier")
    date.registerTempTable("date")
    part.registerTempTable("part")
    for (i <- 0 until table_names.length) {
      sqlContext.cacheTable(table_names(i))
    }

    val Q1="select sum(lo_extendedprice*lo_discount) as revenue from lineorder, date where lo_orderdate = d_datekey and d_year = 1993 and lo_discount between 1 and 3 and lo_quantity < 25"
    val Q2="select sum(lo_extendedprice*lo_discount) as revenue from lineorder, date where lo_orderdate = d_datekey and d_yearmonthnum = 199401 and lo_discount between 4 and 6 and lo_quantity between 26 and 35"
    val Q3="select sum(lo_extendedprice*lo_discount) as revenue from lineorder, date where lo_orderdate = d_datekey and d_weeknuminyear = 6 and d_year = 1994 and lo_discount between 5 and 7 and lo_quantity between 36 and 40"
    val Q4="select sum(lo_revenue), d_year, p_brand1 from lineorder, date, part, supplier where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and p_category = 'MFGR#12' and s_region = 'AMERICA' group by d_year, p_brand1 order by d_year, p_brand1"
    val Q5="select sum(lo_revenue), d_year, p_brand1 from lineorder, date, part, supplier where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and p_brand1 between 'MFGR#2221' and 'MFGR#2228' and s_region = 'ASIA' group by d_year, p_brand1 order by d_year, p_brand1"
    val Q6="select sum(lo_revenue), d_year, p_brand1 from lineorder, date, part, supplier where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and p_brand1 = 'MFGR#2221' and s_region = 'EUROPE' group by d_year, p_brand1 order by d_year, p_brand1"
    val Q7="select c_nation, s_nation, d_year, sum(lo_revenue) as revenue from customer, lineorder, supplier, date where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_region = 'ASIA' and s_region = 'ASIA' and d_year >= 1992 and d_year <= 1997 group by c_nation, s_nation, d_year order by d_year asc, revenue desc"
    val Q8="select c_city, s_city, d_year, sum(lo_revenue) as revenue from customer, lineorder, supplier, date where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and c_nation = 'UNITED STATES' and s_nation = 'UNITED STATES' and d_year >= 1992 and d_year <= 1997 group by c_city, s_city, d_year order by d_year asc, revenue desc"
    val Q9="select c_city, s_city, d_year, sum(lo_revenue) as revenue from customer, lineorder, supplier, date where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and (c_city='UNITED KI1' or c_city='UNITED KI5') and (s_city='UNITED KI1' or s_city='UNITED KI5') and d_year >= 1992 and d_year <= 1997 group by c_city, s_city, d_year order by d_year asc, revenue desc"
    val Q10="select c_city, s_city, d_year, sum(lo_revenue) as revenue from customer, lineorder, supplier, date where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_orderdate = d_datekey and (c_city='UNITED KI1' or c_city='UNITED KI5') and (s_city='UNITED KI1' or s_city='UNITED KI5') and d_yearmonth = 'Dec1997' group by c_city, s_city, d_year order by d_year asc, revenue desc"
    val Q11="select d_year, c_nation, sum(lo_revenue-lo_supplycost) as profit1 from date, customer, supplier, part, lineorder where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_partkey = p_partkey and lo_orderdate = d_datekey and c_region = 'AMERICA' and s_region = 'AMERICA' and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2') group by d_year, c_nation order by d_year, c_nation"
    val Q12="select d_year, s_nation, p_category, sum(lo_revenue-lo_supplycost) as profit1 from date, customer, supplier, part, lineorder where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_partkey = p_partkey and lo_orderdate = d_datekey and c_region = 'AMERICA' and s_region = 'AMERICA' and (d_year = 1997 or d_year = 1998) and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2') group by d_year, s_nation, p_category order by d_year, s_nation, p_category"
    val Q13="select d_year, s_city, p_brand1, sum(lo_revenue-lo_supplycost) as profit1 from date, customer, supplier, part, lineorder where lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_partkey = p_partkey and lo_orderdate = d_datekey and c_region = 'AMERICA' and s_nation = 'UNITED STATES' and (d_year = 1997 or d_year = 1998) and p_category = 'MFGR#14' group by d_year, s_city, p_brand1 order by d_year, s_city, p_brand1"

    var query_array = Array(Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10, Q11, Q12, Q13)
    var times_array:Array[Long] = new Array[Long](query_array.length)

    val q_rdd = sqlContext.sql(query_array(0))
    q_rdd.collect()

    for (i <- 0 until query_array.length) {
      val start = System.currentTimeMillis()
      val q_rdd = sqlContext.sql(query_array(i))
      q_rdd.collect()
      val end = System.currentTimeMillis()
      times_array(i) = end - start
      System.out.println("Completed " + (i + 1) + " in " + times_array(i))
    }

    for (i <- 0 until query_array.length) {
      System.out.println("" + (i + 1) + "," + times_array(i))
    }
  }
}