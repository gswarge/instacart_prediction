name := "instacart-project"

version := "0.5"

scalaVersion := "2.12.10"

val sparkVersion = "3.0.0-preview2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "io.spray" %% "spray-json" % "1.3.3",
  "com.typesafe" % "config" % "1.2.1",
  "org.apache.logging.log4j" %% "log4j-api-scala" % "11.0"

)

resolvers += Classpaths.typesafeReleases

mainClass in(Compile, run) := Some("main.instacart_main")
mainClass in(Compile, packageBin) := Some("main.instacart_main")

