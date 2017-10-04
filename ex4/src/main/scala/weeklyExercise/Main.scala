package weeklyExercise

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object ex4 extends App {
  val conf = new SparkConf().setMaster("local").setAppName("ex3")
  conf.set("spark.driver.host", "localhost");
  val sc = new SparkContext(conf)


  // When searching with Google the results are sorted based on the page rank. The idea of page rank is to mimic 'random surfer'. The random surfer
  // starts browsing from a random page and follows random links from the page to other pages. On each page she continues following the links with probability lambda, and  
  // stops following the links with the probability (1-lambda). In the latter case  she continues browsing starting from a random page.
  //  Page rank is the probability of the random surfer being on page at a random moment.
  //
  // The idea behind computing the page rank is the following, links to the page are contributions that improve the rank of the page,
  // and contributions from higher ranked pages are more important.
  //
  // A simplified page rank works as follows: 
  // 1: initialize the rank for each page to 100/#pages, where #pages is the number of pages
  // 2: loop ten times
  //     for p in pages:
  //       for l in p.links
  //         contribute page l with value p.rank / p.links.size
  //
  //     new rank for each page is (1-lambda)/#pages + lambda*sum(contributions)
  //  
  // This simplified algorithm does not work for dead ends (pages that do not refer to any page) or sources (pages that are not refered at all).
  
  val pages = sc.parallelize(List(
    ("A", List("B")),
    ("B", List("A","C")),
    ("C", List("A","D")),
    ("D", List("A"))))
    
    val numPages = pages.count()
  
  // Task #1: Compute simplified page rank to pages
  // Initialize each page's rank to 0.25
  var ranks = ???

  // Run 10 iterations of PageRank
  ???

  // Print the ranks to stdout
  ranks.???


  // Task #2: Would your solution benefit from caching the data? If yes, how?



  // Indexing makes search engines fast.
  // Inverted index is a mapping from words (of documents) a pair (docId, frequency). So entries of the
  // index are (word, (docId, frequency)), where word is the word, docId is the id of the
  // the document and frequency is the number of the word in the document. Below is five documents with their content:
  //
  // D1: He likes to wink, he likes to drink.
  // D2: He likes to drink, and drink, and drink.
  // D3: The thing he likes to drink is ink.
  // D4: The ink he likes to drink is pink.
  // D5: He likes to wink and drink pink ink.
  // 
  // The inverted index for this set of documents is 
  // he: (1,2), (2,1), (3,1), (4,1), (5,1)
  // ink: (3,1), (4,1), (5,1)
  // pink: (4,1), (5,1)
  // thing: (3, 1)
  // wink: (1,1), (5,1)
  // ...
  // 
  // From the inverted index we can see that the word "thing" appears only once in the document #3, for example.
  //
  // Task #3: Compute inverted index for the documents 1-5 below:
                      
  val rawData = sc.parallelize(List(
      (1, "He likes to wink, he likes to drink."),
      (2, "He likes to drink, and drink, and drink."),
      (3, "The thing he likes to drink is ink."),
      (4, "The ink he likes to drink is pink."),
      (5, "He likes to wink and drink pink ink.")))
  
  val index = rawData.mapValues(_.filterNot(c => c == ',' || c == '.').toLowerCase().split(" "))
  index.collect.foreach(v => {print(v._1 + ": ["); v._2.foreach(s => print(s + ", ")); println("]")})    
                                                            
  val invertedIndex: Map[String, Iterable[(Int, Int)]] = ???

                                                                                                                  
  // Let's try searching with the inverted index:
  twoWordSearch("drink", "pink", invertedIndex, (v1, v2) => v1*v2)
  twoWordSearch("he", "to", invertedIndex, (v1, v2) => v1*v2)  

  // w1 and w2 are search words, invertedIndex is the index, score is the scoring function.
  // Scoring function takes two integers (frequencies of both words in the invertedIndex case) and
  // returns a larger value for better match.
  def twoWordSearch(w1: String, w2: String, theIndex: Map[String, Iterable[(Int, Int)]], score: (Int, Int) => Int) {
    println(f"Searching for '${w1}' and '${w2}'")
    
    val s1 = theIndex(w1)
    val s2 = theIndex(w2)
    var i1 = s1.iterator
    var i2 = s2.iterator

    // Linear merge algorithm:
    var v1 = i1.next
    var v2 = i2.next
    if(v1._1 == v2._1) {println(f"Doc = ${v1._1}, score = ${score(v1._2, v2._2)}")}
    while(i1.hasNext || i2.hasNext) {
      if(v1._1 < v2._1) v1 = i1.next else v2 = i2.next
      if(v1._1 == v2._1) {println(f"Doc = ${v1._1}, score = ${score(v1._2, v2._2)}")}  
    }
  
  }
  
  // Inverted index cannot be used for searching phrases liken "ink wink". Proximity index is used for the task.
  // The idea in the proximity index is to embed position information of each word in the document into the inverted lists.
  // For our example documents the proximity index is 
  //
  // he: (1,0), (1,4) , (2,0), (3,2), (4,2), (5,0)
  // ink: (3,7), (4,1), (5,7)
  // pink: (4,7), (5,6)
  // thing: (3, 1)
  // wink: (1,3), (5,4)
  // ...

  // Task #4: Compute proximity index for the documents 1-5.
  // Hint: 
  var arr = Array("One", "Two", "Three")
  val voila = arr.zipWithIndex.map{ case (s,i) => (s,i) }
  
  val proximityIndex = index.???
  

  // Task #5: Use the twoWordSearch for phrases.
  twoWordSearch("drink", "pink", ???)
  twoWordSearch("likes", "to", ???)
  
}