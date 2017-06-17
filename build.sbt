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

releasePublishArtifactsAction := PgpKeys.publishSigned.value

credentials ++= {
  val settings = new File(System.getProperty("user.home")) / ".m2" / "settings.xml"
  if (settings.exists()) {
    val xml = XML.loadFile(new File(System.getProperty("user.home")) / ".m2" / "settings.xml")
    for (server <- xml \\ "server" if (server \ "id").text == "ossrh") yield {
      Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", (server \ "username").text, (server \ "password").text)
    }
  } else Nil
}

publishTo := {
  if (isSnapshot.value) {
    Some("apache" at "https://oss.sonatype.org/content/repositories/snapshots")
  } else {
    Some("apache" at "https://oss.sonatype.org/service/local/staging/deploy/maven2")
  }
}

pomIncludeRepository := { _ => false }

pomExtra := {
  <url>http://github.com/jongwook/spark-ranking-metrics</url>
  <licenses>
    <license>
      <name>Unlicense</name>
      <url>http://unlicense.org/</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>git@github.com:jongwook/spark-ranking-metrics.git</url>
    <connection>scm:git:git@github.com:jongwook/spark-ranking-metrics.git</connection>
  </scm>
  <developers>
    <developer>
      <id>jongwook</id>
      <name>Jong Wook Kim</name>
      <url>http://jongwook.kim</url>
    </developer>
  </developers>
}
