package weeklyExercise
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object Main extends App {
  val conf = new SparkConf().setMaster("local").setAppName("ex3")
  conf.set("spark.driver.host", "localhost");
  val sc = new SparkContext(conf)
  
  
  // Below is an example of a Scala tuple describing one football match of the Finnish football league (Veikkausliiga) from the season 2016.
  val exampleTuple = ("99","Ilves","HJK","1","0","5050")
  // The first elemement is the id of the game, next is home team, away team, hometeam goals, away team goals and audience.
  // The items of the tuple are refered with _n, where n is the number of the field.
  val gameId = exampleTuple._1
  
  // tupleRdd is an RDD of the matches from Veikkausliiga of the season 2016.
  val rawRdd = sc.textFile("src/main/resources/football/veikkausliiga16.csv")
  val tupleRdd = rawRdd.map(l => {val a = l.split(","); (a(0), a(1), a(2), a(3), a(4), a(5))})
  
  
  // Task #1: transform the tupleRdd to a pair RDD, where key is the home team and value is the audience. 
  val audienceByTeam: RDD[(String, Int)] = ???
  
  // Task #2: Compute the overall audience of the home games for each team
  val totalAudience = ???
  totalAudience.collect.foreach(println)
  
  // Task #3: Compute the average audience of the home games for each team:
  val averageAudience = ???
  averageAudience.collect.foreach(println)
  
  
  // Task #4: File premierLeagueStadiums.csv has a list of the (English) Premier League stadiums in the form statdiumId,stadiumName and
  // premierLeagueClubsWithStadiums.csv has the Premier League teams in the form clubName,stadionId.
  // Based on these produce a pair RDD, whose element are (stadiumName,clubName), where the club plays in the stadium.
  
  val premierLeagueStadiumsRaw = sc.textFile("src/main/resources/football/premierLeagueStadiums.csv")
  val premierLeagueClubsRaw = sc.textFile("src/main/resources/football/premierLeagueClubsWithStadiums.csv")
  
  val j = ???
  j.collect.foreach(println)
  
  // Task #5: File premierLeagueClubs.csv contains the list of current Premier League clubs and the file finns has the list of the
  // Finnish players who have played in some Premier League. The structure of the file is playerName,clubName.
  // Use the two RDDs to compute a pair RDD, whose keys are the current club names and the values are the number of Finnish players played in the club.
  val premierLeagueClubsRaw2 = sc.textFile("src/main/resources/football/premierLeagueClubs.csv")
  val finnsRaw = sc.textFile("src/main/resources/football/finns.csv")
  val finnsInClubs = ???
  finnsInClubs.collect.foreach(println)
  
  
  // Bonus task #1: In football the winning team get three points and loosing one gets 0 points. In case of a draw match both teams get one point.
  // Transform tupleRdd to pair RDD of (team, points) for the matches:
  val pointsPerMatch: RDD[(String, Int)] = ???

  // Bonus task #2: Compute the overall points for each team
  val table: RDD[(String, Int)] = ???
  table.collect.foreach(println)
                                          
}