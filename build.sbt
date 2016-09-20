name := "spark-ranking-metrics"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-mllib" % "2.0.0" % "provided",
  "org.apache.spark" %% "spark-hive" % "2.0.0" % "provided",
  "net.recommenders.rival" % "rival-evaluate" % "0.2",

  "org.apache.spark" %% "spark-mllib" % "2.0.0" % "test",
  "org.apache.spark" %% "spark-hive" % "2.0.0" % "test",
  "org.scalatest" %% "scalatest" % "3.0.0"
)
