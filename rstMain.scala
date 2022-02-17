package cn.hpu

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

object rstMain {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)


    /** ***    集群模式      **** */
    val conf = new SparkConf()
      .set("spark.shuffle.consolidateFiles", "true")
      .set("spark.speculation", "true")
      .set("spark.shuffle.memoryFraction", "0.2")
      .set("spark.storage.memoryFraction", "0.6")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[DimReduction]))

    @transient val sc = new SparkContext(conf)


    val rawDataRDD = sc.textFile(args(0), 40).cache() // 数据路径
    val selectedAttributesIndex = sc.textFile(args(1)) // 依据相似度挑选的属性列名路径
    val savingPath = args(2) // 保存约简结果路径
    val sizeColumns = Try(args(3).toInt).getOrElse(1) // 每块包含的属性数目
    val nbIterIfPerFeat = Try(args(4).toInt).getOrElse(10) // 迭代次数
    val nbBucketDiscretization = Try(args(5).toInt).getOrElse(10) // 离散化块数
    val number = Try(args(6).toInt).getOrElse(5) // 迭代次数（一个相似度属性列）


    val featuresReduction: DimReduction = new DimReduction(sc)
    val arraySplit = Array[Char](',')
    val data2Double: RDD[(Array[Double], Double)] = rawDataRDD
      .mapPartitions(iter => {
        iter.map(_.split(arraySplit))
      })
      .mapPartitions(iter => {
        iter.map(Fcts.string2AttrAndDecision)
      })

    val realData = data2Double.mapPartitions(iter => {
      iter.map(_._1)
    }).cache()
    val label = data2Double.mapPartitions(iter => {
      iter.map(_._2)
    }).cache()
    val discretizedRealRDD = Fcts.discretize(sc, realData, nbBucketDiscretization)
    val readyForDimReduceRDD: RDD[(Array[Double], Double)] = discretizedRealRDD.zip(label).cache()

    val selectedAttributes = selectedAttributesIndex.map(_.split(arraySplit)).map(Fcts.string2array).first()

    rawDataRDD.unpersist(true)
    label.unpersist(true)


    for (num <- 0 until number) {
      realData.unpersist(true)
      val startTime = System.currentTimeMillis()
      val allReduce = for (i <- 0 until nbIterIfPerFeat) yield {
        val (divideByFeatsRDD, columnsOfFeats) = Fcts.divideByFeatures(readyForDimReduceRDD, sizeColumns, selectedAttributes)
        divideByFeatsRDD.cache()
        featuresReduction.roughSetPerFeatsD(divideByFeatsRDD, columnsOfFeats)
      }
      // 求交集
      val reduceFeats = allReduce.reduce(_.intersect(_))
      /** ***    计算时间      **** */
      val endTime = System.currentTimeMillis()
      val duration = endTime - startTime
      println("运行时间： " + duration)

      /** ***    约简信息（ 集群模式 ）   **** */
      // 保存所有迭代产生的约简
      sc.parallelize(reduceFeats).repartition(1).saveAsTextFile(savingPath + "RST-Modified-" + num)

    }

    sc.stop
  }
}
