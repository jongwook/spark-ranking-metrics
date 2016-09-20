name := "spark-ranking-metrics"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq("provided", "test").map { config =>
  "org.apache.spark" %% "spark-mllib" % "2.0.0" % config
} ++ Seq(
  "org.scalatest" %% "scalatest" % "3.0.0" % "test",
  "net.recommenders.rival" % "rival-evaluate" % "0.2" % "test"
)
