name := "nextgendata"

version := "1.0"

scalaVersion := "2.11.8"
val sparkExperimental = "2.0.0-SNAPSHOT"
val sparkStable = "1.6.1"
val sparkVersion = sparkStable

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Spray Repository" at "http://repo.spray.cc/"/*,
  "ASF" at "http://repository.apache.org/snapshots/"*/
)

libraryDependencies ++= Seq( "org.apache.spark" %% "spark-core"      % sparkVersion,
                             "org.apache.spark" %% "spark-sql"       % sparkVersion,
                             "org.apache.spark" %% "spark-streaming" % sparkVersion)

resolvers += Resolver.url("scoverage-bintray", url("https://dl.bintray.com/sksamuel/sbt-plugins/"))(Resolver.ivyStylePatterns)

libraryDependencies ++= Seq("org.scalatest" % "scalatest_2.11" % "2.2.6" % "test")
                      //      "commons-io"    % "commons-io" % "2.5")


