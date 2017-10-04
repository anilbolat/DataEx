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
  val audienceByTeam: RDD[(String, Int)] = tupleRdd.map(t => (t._2, t._6.toInt))
  //audienceByTeam.collect.foreach(println)

  // Task #2: Compute the overall audience of the home games for each team
  val totalAudience = audienceByTeam.reduceByKey((x,y) => (x+y))
  //val totalAudience = audienceByTeam.reduceByKey(_+_) // same
  //totalAudience.collect.foreach(println)

  // Task #3: Compute the average audience of the home games for each team:
  val averageAudience = tupleRdd.map(v => (v._2, (v._6.toInt, 1))). // aralarında işlem yapmak için yanyana koydu!...
    reduceByKey((v1,v2) => ((v1._1+v2._1),(v1._2+v2._2))).mapValues(v => (v._1/v._2))
    // mapvalues: value'lar arası işlem yaptı!...

  val countAudience = tupleRdd.map(t => (t._2, 1)).reduceByKey(_+_)
  //averageAudience.collect.foreach(println)
  //countAudience.collect.foreach(println)

  // Task #4: File premierLeagueStadiums.csv has a list of the (English) Premier League stadiums in the form statdiumId,stadiumName and
  // premierLeagueClubsWithStadiums.csv has the Premier League teams in the form clubName,stadionId.
  // Based on these produce a pair RDD, whose element are (stadiumName,clubName), where the club plays in the stadium.
  
  val premierLeagueStadiumsRaw = sc.textFile("src/main/resources/football/premierLeagueStadiums.csv")
  val premierLeagueClubsRaw = sc.textFile("src/main/resources/football/premierLeagueClubsWithStadiums.csv")
  
  val stadiums = premierLeagueStadiumsRaw.map(l => {val v = l.split(','); (v(0), v(1))}) // (id,name-std)
  val clubs = premierLeagueClubsRaw.map(l => {val v = l.split(","); (v(1), v(0))}) // (id,name-club)
  val j = stadiums.join(clubs) // (id, (name-std,name-club))
    .map(v => v._2) // (name-st,name-club)
  //j.collect.foreach(println)
  
  // Task #5: File premierLeagueClubs.csv contains the list of current Premier League clubs and the file finns has the list of the
  // Finnish players who have played in some Premier League. The structure of the file is playerName,clubName.
  // Use the two RDDs to compute a pair RDD, whose keys are the current club names and the values are the number of Finnish players played in the club.
  val premierLeagueClubsRaw2 = sc.textFile("src/main/resources/football/premierLeagueClubs.csv") // name-club
  val finnsRaw = sc.textFile("src/main/resources/football/finns.csv") // name-player,name-club

  //val finnsInClubs = finnsRaw.map(l => {val v = l.split(","); (v(1),1)}).  // (name-club,1)
    //        reduceByKey(_+_)  // (name-club, num-of-finn-players)
  //finnsInClubs.collect.foreach(println)

  val clubNames = premierLeagueClubsRaw2.map(l => (l,0)) // (name-club,0)
  val finns = finnsRaw.map(l => {val v = l.split(','); (v(1), v(0))})   // (name-club,name-player)
  val finnsInClubs = clubNames.leftOuterJoin(finns) // (A + (A n B)) kumeleme... // (name-club, (0,Some(name-player))) or (name-club, (0,None))
      .map(v => (v._1, v._2._2 match {case Some(n) => 1; case None => 0})).reduceByKey(_+_)
//  finnsInClubs.collect.foreach(println)
  
  // Bonus task #1: In football the winning team get three points and loosing one gets 0 points. In case of a draw match both teams get one point.
  // Transform tupleRdd to pair RDD of (team, points) for the matches:
  val pointsPerMatch: RDD[(String, Int)] = tupleRdd.flatMap(v => {if (v._4.toInt > v._5.toInt) Array((v._2,3),(v._3,0))
                                                              else if (v._4.toInt < v._5.toInt)  Array((v._3,3),(v._2,0))
                                                              else  Array((v._2,1),(v._3,1))
  })
  //pointsPerMatch.collect.foreach(print)

  val pointsPerMatchOriginalSolution: RDD[(String, Int)] = tupleRdd.flatMap(t => {val d = t._4.toInt - t._5.toInt;
    if (d>0) Array((t._2, 3), (t._3, 0))
    else if (d == 0) Array((t._2, 1), (t._3, 1))
    else Array((t._2, 0), (t._3, 3))})
  //pointsPerMatchOriginalSolution.collect.foreach(print)

  // Bonus task #2: Compute the overall points for each team
  val table: RDD[(String, Int)] = pointsPerMatch.reduceByKey(_+_)
  table.collect.foreach(println)
                                          
}