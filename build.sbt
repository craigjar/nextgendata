name := "nextgendata"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1"

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Spray Repository" at "http://repo.spray.cc/")

resolvers += Resolver.url("scoverage-bintray", url("https://dl.bintray.com/sksamuel/sbt-plugins/"))(Resolver.ivyStylePatterns)

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.6" % "test"

