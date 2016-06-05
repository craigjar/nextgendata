name := "nextgendata"

version := "1.0"

scalaVersion := "2.11.8"
val sparkExperimental = "2.0.0-SNAPSHOT"
val sparkStable = "1.6.1"
val sparkVersion = sparkStable

scalacOptions ++=Seq("-explaintypes", "-unchecked", "-deprecation",
  "-Ywarn-dead-code", "-Ywarn-value-discard", "-Ywarn-adapted-args", "-Ywarn-infer-any",
  "-Ywarn-numeric-widen", "-Ywarn-nullary-unit", "-Ywarn-nullary-override",
  "-Ywarn-inaccessible")

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Spray Repository" at "http://repo.spray.cc/",
  "scoverage-bintray" at "https://dl.bintray.com/sksamuel/sbt-plugins/"/*,
  "ASF" at "http://repository.apache.org/snapshots/"*/
)

libraryDependencies ++= Seq( "org.apache.spark" %% "spark-core"      % sparkVersion,
                             "org.apache.spark" %% "spark-sql"       % sparkVersion,
                             "org.apache.spark" %% "spark-streaming" % sparkVersion,
                             "org.scalatest"    %% "scalatest"       % "2.2.6" % "test")
