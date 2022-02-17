package cn.hpu

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.util.Random


class DimReduction(@transient val sc: SparkContext) extends Serializable {


  def roughSetPerFeatsD(data:RDD[(Array[Array[Double]], Double)],columnsOfFeats:Array[Array[Int]]) = {

    val dataBC = sc.broadcast(data.collect)

    val reduct = sc.parallelize(0 until columnsOfFeats.length).map( numColumn => {

      val dataPerFeatArray = dataBC.value.map { case ( vectorPerFeats, label) => ( vectorPerFeats(numColumn), label) }

      val originalFeatures = columnsOfFeats(numColumn)

      if(originalFeatures.length == 1){
         Array(originalFeatures)
      }else{
        val mapOriginalFeatures = originalFeatures.zipWithIndex.map(_.swap).toMap

        val features = (0 until originalFeatures.size).toArray
        val allCombinations = (1 until features.size).flatMap(features.combinations).toArray // to :考虑分区全部属性; until :不考虑分区全部属性
        val allReductSet = roughSetCore(dataPerFeatArray, allCombinations)
        //      val data = sc.parallelize(dataPerFeatArray)
        //      val allReductSet1 = roughSetCoreGroup(sc,dataPerFeatArray, allCombinations)
        allReductSet.map(_.map(mapOriginalFeatures))
      }

    }).map( allReduct => allReduct(Random.nextInt(allReduct.size)) ).fold(Array.empty[Int])(_ ++ _)
    reduct
  }


  def roughSetCoreGroup(sc:SparkContext,data: Array[(Array[Double], Double)], allCombinations: Array[Array[Int]]): Array[Array[Int]] = {
    
    val data1 = sc.parallelize(data)
    val dependencyAll = allCombinations.map(f => (f, f.size,calculateDependencyWithGroupByKey(f, data1)))
    var dependencyMax = dependencyAll.maxBy(_._3)._3
    val allDependencyMax = dependencyAll.filter(l => l._3 == dependencyMax)
    val maxDependencyMinFeatureNb = allDependencyMax.minBy(_._2)._2
    val allReductSet = allDependencyMax.filter{ case(_, sizeFeatures, _) => sizeFeatures == maxDependencyMinFeatureNb }.map{ case(features, _, _) => features }
    allReductSet

  }


  def roughSetCore(data:Array[( Array[Double], Double)], allCombinations:Array[Array[Int]]) = {

//      val dependencyAll = allCombinations.map(f => (f, f.size, calculateDependency(f, data)))
      val dependencyAll = allCombinations.map(f => (f, f.size, calculateDependencyWithGroupBy(f, data)))
      //    val data1 = sc.parallelize(data)
      //    val dependencyAll = allCombinations.map(f => (f, f.size,calculateDependencyWithReduceByKey(f, data1)))
      var dependencyMax = dependencyAll.maxBy(_._3)._3
      val allDependencyMax = dependencyAll.filter(l => l._3 == dependencyMax)
      val maxDependencyMinFeatureNb = allDependencyMax.minBy(_._2)._2
      val allReductSet = allDependencyMax.filter{ case(_, sizeFeatures, _) => sizeFeatures == maxDependencyMinFeatureNb }.map{ case(features, _, _) => features }
      allReductSet
  }


  def isUnique(item :(ArrayBuffer[Double], Iterable[Double])):Int  = {
    val labels = item._2.toArray
    val len = labels.distinct.length
    if(len == 1){
     val t = labels.length
      t
    }else{
      0
    }

    }


  val calculateDependencyWithGroupByKey = (f : Array[Int], data: RDD[(Array[Double], Double)] )  =>
    {
      // 提取属性元素
      val featuresAndLabel = data.map{ case(vector, label) => (keyValueExtract(f, vector), label)}
      val featruesGroup = featuresAndLabel.groupByKey()
      val uniqueValues = featruesGroup.map(isUnique)
      uniqueValues.sum().toInt
    }


  val calculateDependencyWithReduceByKey = (f : Array[Int], data: Array[(Array[Double], Double)] )  =>
  {
    // 提取属性元素
    val featuresAndLabel = data.map{ case(vector, label) => (keyValueExtract(f, vector), label)}


    val featuresAndLabelRDD: RDD[(ArrayBuffer[Double], Double)] = sc.parallelize(featuresAndLabel)
    val featruesGroup = featuresAndLabelRDD.map(item => (item._1, ArrayBuffer(item._2))).reduceByKey(_ ++= _)


    val uniqueValues = featruesGroup.map(isUnique1)
    uniqueValues.sum().toInt
  }


  def isUnique1(item :(ArrayBuffer[Double], ArrayBuffer[Double])):Int  = {

    val len = item._2.distinct.length
    if(len == 1){
      item._2.length
    }else{
      0
    }

  }


  val calculateDependencyWithGroupBy = (f : Array[Int], data: Array[(Array[Double], Double)] )  =>{
    // 提取属性元素

    val featuresAndLabel = data.map{ case(vector, label) => (keyValueExtract(f, vector), label)}
    val dataArray: Array[(ArrayBuffer[Double], Double)] = featuresAndLabel
    // 属性数目(条件属性+决策属性)

    val sum = dataArray.map{ case(attr, dec) => (attr, dec) }
      .groupBy(_._1)
      .map{ case(label, objects) => objects.map(_._2).toList}.toArray
      .filter(item => item.distinct.length == 1)
      .map(item => item.length)
      .reduce(_ + _)



//    val decsionValuesArray = dataArray.map{ case(attr, dec) => (attr, dec) }
//        .groupBy(_._1)
//        .map{ case(label, objects) => objects.map(_._2).toList}.toArray
//    val sum = decsionValuesArray
//              .map(elementsAreSame)
//              .reduce(_ + _)
    sum

  }

  def elementsAreSame(f: List[Double]): Int = {
    if(f.distinct.length == 1){
      f.length
    }else{
      0
    }
  }


  val calculateDependency = (f : Array[Int], data: Array[(Array[Double], Double)] )  =>
  {
    // 提取属性元素
    val featuresAndLabel = data.map{ case(vector, label) => (keyValueExtract(f, vector), label)}
    // 直接计算依赖度
    // 数据集
    val dataArray: Array[(ArrayBuffer[Double], Double)] = featuresAndLabel
    // 属性数目(条件属性+决策属性)
    val len = dataArray(0)._1.length + 1

    val grid = ArrayBuffer.empty[Array[Double]]

    // 数据数目
    val dataNumber = dataArray.length

    for (i <- 0 until dataNumber){

      val intArray = new Array[Double](len + 2)

      // 整合条件属性和决策为一体
      val attriburtesLabel = new Array[Double](len)
      // 单独一个实例
      val dataObject: (ArrayBuffer[Double], Double) = dataArray(i)

      for(i <- 0 until (len -1)){
        attriburtesLabel(i) = dataObject._1(i)
      }
      attriburtesLabel(len-1) = dataObject._2


      // grid 为空
      if(i == 0){
//        for(j  <- 0 until len){
//          intArray(i) = attriburtesLabel(j)
//        }
//        intArray(len) = 1
//        intArray(len + 1) = 0
//        grid.append(intArray)
        grid.append(gridElement(attriburtesLabel, len))
      }
      else{
        var signal = 0
        for(g <- grid if signal == 0){
          val attributes = g.slice(0,len-1)
          val label = g(len-1)
          if( attributes.sameElements(attriburtesLabel.slice(0,len-1)) && (label == attriburtesLabel(len-1)) ){
            g(len) = g(len) + 1.0
            signal = 1
          }else if (attributes.sameElements(attriburtesLabel.slice(0,len-1)) && (label != attriburtesLabel(len-1))){
            g(len) = g(len) + 1.0
            g(len+1) = 1.0
            signal = 1
          }
        }

        if(signal == 0){
//          for(i  <- 0 until len){
//            intArray(i) = attriburtesLabel(i)
//          }
//          intArray(len) = 1
//          intArray(len + 1) = 0
//
//          grid.append(intArray)
          grid.append(gridElement(attriburtesLabel, len))

        }
      }
    }

    // 计算依赖度
    var sum = 0.0
    for(i <- grid){
      if(i(len+1) == 0.0){
        sum = sum + i(len)
      }
    }
    sum.toInt
  }


  def gridElement(attriburtesLabel: Array[Double], len:Int):Array[Double] = {
    val intArray = new Array[Double](len + 2)
    for(i  <- 0 until len){
      intArray(i) = attriburtesLabel(i)
    }
    intArray(len) = 1.0
    intArray(len + 1) = 0.0

    intArray

  }


  def keyValueExtract(f : Array[Int], vector : Array[Double]):ArrayBuffer[Double] = {
    val key = ArrayBuffer.empty[Double]
    for (i <- 0 until f.size){
      key += vector(f(i))
    }
    key
  }

}
