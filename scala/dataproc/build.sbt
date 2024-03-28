val scala2Version = "2.13.13"

lazy val root = project
  .in(file("."))
  .settings(
    name := "dataproc",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala2Version,

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.1",
      "org.apache.spark" %% "spark-sql" % "3.5.1"
    )
  )
