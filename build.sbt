name := "instacart-project"

version := "0.5"

scalaVersion := "2.12.10"

val sparkVersion = "3.0.0-preview2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "compile",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "compile",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "compile",
  "io.spray" %% "spray-json" % "1.3.3",
  "com.typesafe" % "config" % "1.2.1",
  "org.apache.logging.log4j" %% "log4j-api-scala" % "11.0"

)

resolvers += Classpaths.typesafeReleases

mainClass in(Compile, run) := Some("main.instacartMain")
mainClass in(Compile, packageBin) := Some("main.instacartMain")

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
