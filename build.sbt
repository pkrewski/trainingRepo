name := "NA-spark"

fork :=true

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.10" % "2.2.1",
  "org.apache.spark" % "spark-core_2.10" % "1.5.1" % "provided",
  "org.apache.spark" % "spark-sql_2.10" % "1.5.1" % "provided",
  "org.apache.spark" % "spark-hive_2.10" % "1.5.1" % "provided",
  "mysql" % "mysql-connector-java" % "5.1.38" % "provided",
  "log4j" % "log4j" % "1.2.15" exclude("javax.jms", "jms") exclude("com.sun.jdmk", "jmxtools") exclude("com.sun.jmx", "jmxri")
)

excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
 cp filter {x => x.data.getName.matches("sbt.*") || x.data.getName.matches(".*macros.*") || x.data.getName.matches("spark.*")}
}
