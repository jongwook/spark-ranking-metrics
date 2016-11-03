import scala.xml.XML

organization := "com.github.jongwook"

name := "spark-ranking-metrics"

isSnapshot := version.value.endsWith("-SNAPSHOT")

scalaVersion := "2.11.8"

libraryDependencies ++= Seq("provided", "test").map { config =>
  "org.apache.spark" %% "spark-mllib" % "2.0.0" % config
} ++ Seq(
  "org.scalatest" %% "scalatest" % "3.0.0" % "test",
  "net.recommenders.rival" % "rival-evaluate" % "0.2" % "test"
)

credentials ++= {
  val xml = XML.loadFile(new File(System.getProperty("user.home")) / ".m2" / "settings.xml")
  for (server <- xml \\ "server" if (server \ "id").text == "ossrh") yield {
    Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", (server \ "username").text, (server \ "password").text)
  }
}

publishTo := {
  if (isSnapshot.value) {
    Some("apache" at "https://oss.sonatype.org/content/repositories/snapshots")
  } else {
    Some("apache" at "https://oss.sonatype.org/service/local/staging/deploy/maven2")
  }
}
