name :=  "SparkStreamingProject"

version := "1.0"

scalaVersion := "2.11.8"

val sparkDependencies = Seq(
	"org.apache.spark" %% "spark-sql" % "2.4.1" % "provided",
	"org.apache.spark" %% "spark-core" % "2.4.1" % "provided",
	"org.apache.spark" %% "spark-streaming" % "2.4.1" % "provided",
	"org.elasticsearch" %% "elasticsearch-spark-20" % "7.7.0" % "provided",
	"com.databricks" %% "spark-xml" % "0.10.0")


libraryDependencies ++= sparkDependencies
