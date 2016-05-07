name := "spring-batch-partion-scala"
version := "1.0"
scalaVersion := "2.11.8"

enablePlugins(JavaAppPackaging)

libraryDependencies ++= Seq(
"org.springframework.boot" % "spring-boot-starter-batch" % "1.3.3.RELEASE",
"redis.clients" % "jedis" % "2.8.1",
  "org.springframework.data" % "spring-data-redis" % "1.7.1.RELEASE",
  "com.alibaba" % "druid" % "1.0.15",
  "postgresql" % "postgresql" % "9.1-901.jdbc4",

  "commons-dbcp" % "commons-dbcp" % "1.4"

)

//enablePlugins(JDKPackagerPlugin)

mainClass in Compile := Some("com.yuanqingfei.batch.Application")
