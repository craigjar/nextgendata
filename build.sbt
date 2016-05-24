name := "nextgendata"

version := "1.0"

scalaVersion := "2.11.8"

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Spray Repository" at "http://repo.spray.cc/"/*,
  "ASF" at "http://repository.apache.org/snapshots/"*/
)

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"
//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0-SNAPSHOT"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1"
//libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0-SNAPSHOT"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.1"
//libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.0.0-SNAPSHOT"

resolvers += Resolver.url("scoverage-bintray", url("https://dl.bintray.com/sksamuel/sbt-plugins/"))(Resolver.ivyStylePatterns)

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.6" % "test"

libraryDependencies += "commons-io" % "commons-io" % "2.5"


