//author : Jonathan Laksamana Purnomo
//NPM : 2016730081
object bigData {
  def main(args: Array[String]): Unit = {
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local[*]")
      .appName("Movie Rating Experiment")
      .getOrCreate
    val data = spark.read
      .format("csv")
      .option("header", "true")
      .load("../../../../imdb_movie_5000.csv")

    data.select(data.col("*")).createTempView("df_movies")
    val OUTPUT_BASE_PATH = "../output/"
    //    no 1
    val sql_top_10_productive_director = "SELECT" +
      " director_name , count(movie_title) as count_movie" +
      " from df_movies" +
      " where director_name is not null " +
      " group by director_name " +
      " order by count_movie desc LIMIT 10"
    var query = spark.sql(sql_top_10_productive_director)
    query.show()
    query.write.json(OUTPUT_BASE_PATH + "top_10_director_name")
    //    no 2
    val sql_top_ten_gross_by_movie = "SELECT" +
      " movie_title , gross " +
      " FROM df_movies ORDER BY" +
      " gross desc limit 10 "
    query = spark.sql(sql_top_ten_gross_by_movie)
    query.show()
    query.write.json(OUTPUT_BASE_PATH + "top_10_gross_by_movie")
    //    no 3
    val sql_average_imbd_by_first_actor_name = "SELECT" +
      " actor_1_name ,  avg(imdb_score) as average_imbd_score " +
      " FROM df_movies group by actor_1_name "
    query = spark.sql(sql_average_imbd_by_first_actor_name)
    query.show()
    query.write.json(OUTPUT_BASE_PATH + "average_imdb_score_by_first_actor_name")

  }
}


















