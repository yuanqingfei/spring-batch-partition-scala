package com.yuanqingfei.batch.pojo

import scala.beans.BeanProperty

/**
  * Created by aaron on 16-5-8.
  */
class RedisProduct {

  @BeanProperty
  var key: String = null

  @BeanProperty
  var value: String = null

  override def toString: String = {
    return "RedisProduct [key=" + key + ", value=" + value + "]"
  }

}
