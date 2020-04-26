name := "Scala_Final_Project"

version := "0.1"

scalaVersion := "2.11.12"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"

val sparkVersion = "2.4.3"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
resolvers ++= Seq(
  DefaultMavenRepository,
  Resolver.bintrayRepo("typesafe", "releases"),
  Resolver.sonatypeRepo("releases"),
  Resolver.mavenLocal
)

/* ... */
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.0" % "provided",
  "org.apache.commons" % "commons-lang3" % "3.3.2")
