package com.yuanqingfei.batch.pojo

import scala.beans.BeanProperty

/**
  * Created by aaron on 16-5-8.
  */
class OfbizProduct {

  @BeanProperty
  var id: String = null

  @BeanProperty
  var name: String = null

  @BeanProperty
  var category: String = null

  override def toString: String = {
    return "OfbizProduct{" + "id='" + id + '\'' + ", name='" + name + '\'' + ", category='" + category + '\'' + '}'
  }

}
