package com.yuanqingfei.batch

import org.springframework.boot.SpringApplication

/**
  * Created by aaron on 16-5-3.
  */
object Application extends App{

  val configuration : Array[Object] = Array(classOf[MyBatchApplication])
  System.exit(SpringApplication.exit(SpringApplication.run(configuration, args)));
}
