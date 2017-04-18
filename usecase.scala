package geo.company.dept

import java.text.SimpleDateFormat
import java.util.Date

import geo.company.sparkavro.MyKryoRegistrator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by cloudera on 3/29/17.
  */
object WettestMsaYearMonth {

  //parsing logic for precipitation file
  def parsePrecipLine(line:String)= {
    val fields = line.split(",")
    val wban = fields(0)
    val yearmonthday= fields(1)
    val hour = fields(2)
    val precipitation= fields(3).trim()
    (wban, yearmonthday, hour,precipitation)
  }

  //parsing logic for Station file
  def parseStationLine(line:String)= {
    val fields = line.split("\\|")
    val wban = fields(0)
    val city= fields(6)
    val state = fields(7)
    (wban, city, state)
  }

  //parsing String to Double
  def parseDouble(s: String): Double = try {s.toDouble} catch { case _ => 0.0 }

  //truncating Double precision
  def truncateAt(n: Double, p: Int): Double = { val s = math pow (10, p); (math floor n * s) / s }

  /** Load up a Map of station IDs to citystate */
  def loadStationNames() : Map[Int, String] = {

    // Create a Map of Ints to Strings, and populate it from u.item.
    var stationNames:Map[Int, String] = Map()

    val lines = scala.io.Source.fromFile("/home/cloudera/hadoopproject/201505station.txt").getLines()
    for (line <- lines) {
      var fields = line.split("\\|")
      if (fields.length > 1 && fields(0)!="WBAN" && fields(0).length>0) {
        if (fields(6).length.equals(0)){fields(6)="unknown"}
        if (fields(7).length.equals(0)){fields(7)="unknown"}

        stationNames += (fields(0).toInt -> (fields(6)+", "+fields(7)))
      }
    }

    return stationNames

  }

  ////parsing logic for City_State_Msa file
  def parseCityStateMsa(line:String)= {
    val fields = line.split("\\t")
    val cityState= fields(0).trim+ ", "+ fields(1).trim
    val msa= fields(2)
    (cityState.toUpperCase,msa.replaceAll("\"","").trim)
  }

  //parsing logic for MSA_population file
  def parseMsaPop(line:String)= {
    val fields = line.split(",")
    if (fields.length >1) {
      val msa = fields(3).trim.replaceAll("\"", "")
      val state = fields(4).trim.replaceAll("\"", "")
      val pop = fields(13).trim
      val lsad= fields(5).trim
      (msa.replaceAll("\"", "")+", "+state, pop,lsad)

    }
    else {
      ("unknown","unknown","unknown")
    }
  }

  def main(args: Array[String]) = {
    val sparkConf = new SparkConf().setAppName("spark table write process")  // DONE - master (local or YARN) is set while submitting the job
      .set("spark.hadoop.validateOutputSpecs", "false").set("spark.storage.memoryFraction", "1")
      .set("spark.sql.parquet.compression.codec","gzip")
    MyKryoRegistrator.register(sparkConf)
    val sc = new SparkContext(sparkConf)
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive","true") //Enables reading directory recursively
    val blockSize = 1024 * 1024 * 256      // Same as HDFS block size
    /*Intializing the spark context with block size values*/
    sc.hadoopConfiguration.setInt( "dfs.blocksize", blockSize )
    sc.hadoopConfiguration.setInt( "parquet.block.size", blockSize )

    val sqlContext = new SQLContext(sc)
    val  hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val vconf = new Configuration()
    val fs = FileSystem.get(vconf)
    //for toDF
    import sqlContext.implicits._

    val regexStrip = "[{}']".r
    val theConfigFileName = args(0)

    val paramFile = sc.textFile(theConfigFileName).filter(f => (f.trim.length() > 0 && f.substring(0,1) != "#"))
      .map(line =>  regexStrip.replaceAllIn(line, ""))
    val parameters = paramFile
      .map (line => {
        val fields = line.split("=")
        (fields(0).toString,fields(1).toString)
      })

    val pm=parameters.collect
    val p = Map(pm:_*)

    val dateFormatter = new SimpleDateFormat("yyyyMMddhhmmss")
    val submittedDateConvert = new Date()
    val run_id = dateFormatter.format(submittedDateConvert)
   // val p_hms_server = p.apply("PRECIP_FILE_LOCATION")
   val p_hms_server = p.apply("HIVE_METASTORE_URL")
    val p_output = p.apply("OUTPUT_FILE_LOCATION")
    val p_msa = p.apply("CITY_STATE_MSA_FILE_LOCATION")
    val p_precip = p.apply("PRECIP_FILE_LOCATION")
    val p_nn_server = p.apply("NAME_NODE_URL")
    val dbname = p.apply("HIVE_FACT_DB")
    val reducer = p.apply("p_numReducers")
    val oppath = p_nn_server + "/user/cloudera/sparklearn/output/"
    //load precipitation file in Rdd from HDFS
    val precipLines = sc.textFile(p_nn_server + p_precip)
   // val precipLines = sc.textFile(p_nn_server + "/user/cloudera/hadoopproject/input/201505precip.txt")
    //parse Precipitation file using parsePrecipLine function
    val parsedPrecipLines = precipLines.map(parsePrecipLine)
    //ignoring header column and data between 01 and 07 hour
    val ignoreNightTime= parsedPrecipLines.filter(x=>x._1!="Wban" && x._3!="01" && x._3!="02" && x._3!="03" && x._3!="04" && x._3!="05" && x._3!="06" && x._3!="07")
    //ignoring no value column and data that had String "T" in it
    val ignoreNoValueCol = ignoreNightTime.filter(x=>x._4.length()>0 && x._4!="T")
    //map the output to a format needed for doing an aggregation
    //val cleanedPrecipData= ignoreNoValueCol.map(x=>(x._1,parseDouble(x._4)))
    //get wban,year,month,rainfall and map the output to a format needed for doing an aggregation
    val cleanedPrecipData= ignoreNoValueCol.map(x=>(x._1,(x._2.substring(0,4)),(x._2.substring(4,6)),parseDouble(x._4)))
    //cleanedPrecipData.foreach(println)

    //define which columns belong to the key and which to the value.
    val mapMultiColumnKey= cleanedPrecipData.map{case (wban,year,month,precipitaton)=> ((wban,year,month),(precipitaton))}
    //do sum aggregation by using reducebykey
    val sumPrecipByStation= mapMultiColumnKey.reduceByKey((x,y)=>x+y)
    val resultsPrecipData= sumPrecipByStation.collect()
    // Create a broadcast variable of our StationsId ->CityState map
    val stationNameDict = sc.broadcast(loadStationNames)
    //use broadcast variable to do a lookup
    val sumPrecipByStationName=sumPrecipByStation.map(x=>(stationNameDict.value(x._1._1.toInt),x._1._2,x._1._3,x._2))
    //sumPrecipByStationName.foreach(println)
    //load citystate->msa
    //val cityStateMsaLines = sc.textFile(p_nn_server + "/user/cloudera/hadoopproject/input/city_state_msa_fixed.txt")
    val cityStateMsaLines = sc.textFile(p_nn_server + p_msa)
    //parde city_State_msa data
    val parsedCityStateMsaLines= cityStateMsaLines.map(parseCityStateMsa)
    //converting rainfall data and citystatemsa to df for join
    //val sumRainfallByCityStateDf=sumPrecipByStationName.toDF("citystate","rainfallamount")
    val sumRainfallByCityStateDf=sumPrecipByStationName.toDF("citystate","year","month","rainfallamount")
    //convert city_state_msa to df for join
    val parsedCityStateMsaDf=parsedCityStateMsaLines.toDF("citystate","msa")
    //do inner join of rainfall_citystate data to city_state_msa data
    val joineddf= sumRainfallByCityStateDf.join(parsedCityStateMsaDf,sumRainfallByCityStateDf("citystate")===parsedCityStateMsaDf("citystate"))
    //create new df for total rainfall by MSA
    val totRainfallByMsaDf= joineddf.groupBy("msa","year","month").sum("rainfallamount").withColumnRenamed("sum(rainfallamount)", "totRainfall")
    //now we need to load MSA to Population data and parse it for final step
    val msaPopLines= sc.textFile(p_nn_server + "/user/cloudera/hadoopproject/input/cbsa-est2016-alldata_cleaned.csv")
    //parse the msapopLines
    val parsedMsaPopLines= msaPopLines.map(parseMsaPop)
    //filter the header and get all lines which are MSA qualified
    val filterParsedMsaPopLines=parsedMsaPopLines.filter(x=>x._1!="unknown" && x._3.equals("Metropolitan Statistical Area")).map(x=>(x._1,x._2))
    //convert them to DF for join
    val msaPopDf= filterParsedMsaPopLines.toDF("msa","pop")
    //do the final join between rainfall_msa to pop_msa df
    val finaljoinedDf= msaPopDf.join(totRainfallByMsaDf,msaPopDf("msa")===totRainfallByMsaDf("msa")).select(msaPopDf("msa"),msaPopDf("pop"),totRainfallByMsaDf("year"),totRainfallByMsaDf("month"),totRainfallByMsaDf("totRainfall"))
    //add the column wettestpopulation as product of pop and rainfall for a MSA
    val df_wettestmsa= finaljoinedDf.withColumn("WettestPopulation",finaljoinedDf("pop").cast("Int")*finaljoinedDf("totRainfall"))
    //doing sort first and then repartitioning to 1 to create only one file as data for MSA will never be greater than 386
    val df_wettestmsa_repartitioned= df_wettestmsa.sort("WettestPopulation").repartition(1)
    // store it finally on HDFS in CSV formation using databricks library
   // df_wettestmsa_repartitioned.write.format("com.databricks.spark.csv").option("delimiter", "\t").mode("overwrite").save(p_nn_server +"/user/cloudera/hadoopproject/output/wettestpopulation")
    df_wettestmsa_repartitioned.write.format("com.databricks.spark.csv").option("delimiter", "\t").mode("overwrite").save(p_nn_server + p_output)

    sc.stop()


  }

}
