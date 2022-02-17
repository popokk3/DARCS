package cn.hpu

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.math.{max, min}
import scala.util.Random

object Fcts extends Serializable
{
  def string2AttrAndDecision(str: Array[String]):(Array[Double],Double ) = {

    val doubleArray = new Array[Double](str.length - 1)
    val deci = str(0).toDouble
    for(i <- 1 until str.length){
      doubleArray(i - 1) = str(i).toDouble
    }
    (doubleArray,deci)
  }


  def string2array(str: Array[String]):(Array[Int] ) = {

    val intArray = new Array[Int](str.length)
    for(i <- 0 until str.length){
      intArray(i) = str(i).toInt
    }
    intArray
  }


// toMap
  def selectAttributes(groupCosineValueBC:Broadcast[Array[(Long, ArrayBuffer[(Long, Double)])]], nbFeatures: Int, similarityValue: Double):Array[Int] = {
    // 生成一个列名列表
    val columnIndex = ArrayBuffer.empty[Long]
    for (i <- 0 until nbFeatures) {
      columnIndex.append(i.toLong)
    }

    // 保存所有根据余弦值聚集的列名信息
    val allColumnBucks = ArrayBuffer.empty[Array[Long]]
    // 保存已经选过的列名信息
    val selectedColumnArray = ArrayBuffer.empty[Long]


    val groupCosineValueBCValue = groupCosineValueBC.value.toMap


    for(i <- columnIndex){
      // 每个key聚类中满足条件的列号
      val elementArray = ArrayBuffer.empty[Long]

      if(!selectedColumnArray.contains(i)){
        elementArray.append(i)
        val groupCosineValue = groupCosineValueBCValue.get(i)
        for(j: ArrayBuffer[(Long, Double)] <- groupCosineValue){
          for(t <- j){
            if(t._2 >= similarityValue){
              elementArray.append(t._1)
            }
          }
        }
      }

      val uniqueColumns = elementArray -- selectedColumnArray

      if(uniqueColumns.length > 1){
        selectedColumnArray ++= uniqueColumns
        allColumnBucks.append(uniqueColumns.toArray)
      }
    }


    val finalSelectedColumnArray = columnIndex -- selectedColumnArray   // 不满足相似度值的元素

    // 提取元素(随机挑选一个)，判断len==1的条件可以省略
    for(i <- allColumnBucks){
      val len = i.length
      finalSelectedColumnArray.append(i(Random.nextInt(len)))
    }

    finalSelectedColumnArray.map(_.toInt).toArray

  }







//  def getLong(tuple: (Long, Double), similarityValue: Double): Long= {
//    if(tuple._2 >= similarityValue){
//      tuple._1
//    }else{
//      null
//    }
//
//  }

//  def func(groupCosineValue: (Long, ArrayBuffer[(Long, Double)]), i: Long, similarityValue: Double): ArrayBuffer[Long] = {
//    if(groupCosineValue._1 == i){
//      val t: ArrayBuffer[Long] = groupCosineValue._2.map(getLong(_, similarityValue))
//      t
//    }else{
//      ArrayBuffer.empty[Long]
//    }
//
//  }



//  def selectAttributes3(groupCosineValueBC: Broadcast[Array[(Long, ArrayBuffer[(Long, Double)])]], nbFeatures: Int, similarityValue: Double):Array[Int] = {
//    // 生成一个列名列表
//    val columnIndex= ArrayBuffer.empty[Long]
//    for (i <- 0 until nbFeatures){
//      columnIndex.append(i.toLong)
//    }
//
//    // 保存所有根据余弦值聚集的列名信息
//    val allColumnBucks = ArrayBuffer.empty[Array[Long]]
//    // 保存已经选过的列名信息
//    val selectedColumnArray = ArrayBuffer.empty[Long]
//
//
//    for( i <- columnIndex){
//      if(!selectedColumnArray.contains(i)){
//        groupCosineValueBC.value.map(func(_,i,similarityValue))
//
//
//
//      }
//
//
//    }
//
//
//
//  }



//    def selectAttributes2(groupCosineValueBC: Broadcast[Array[(Long, ArrayBuffer[(Long, Double)])]], nbFeatures: Int,similarityValue: Double):Array[Int] = {
//
//    // 生成一个列名列表
//    val columnIndex= ArrayBuffer.empty[Long]
//    for (i <- 0 until nbFeatures){
//      columnIndex.append(i.toLong)
//    }
//
//    // 保存所有根据余弦值聚集的列名信息
//    val allColumnBucks = ArrayBuffer.empty[Array[Long]]
//    // 保存已经选过的列名信息
//    val selectedColumnArray = ArrayBuffer.empty[Long]
//
//    for(i <- groupCosineValueBC.value){
//      // 每个key聚类中满足条件的列号
//      val elementArray = ArrayBuffer.empty[Long]
//      if(!selectedColumnArray.contains(i._1)){
//        elementArray.append(i._1)
//        for(j <- i._2){
//          if(j._2 >= similarityValue){
//            elementArray.append(j._1)
//          }
//        }
//        val uniqueColumns = elementArray -- selectedColumnArray
//
//        if(uniqueColumns.length > 1){
//          selectedColumnArray ++= uniqueColumns
//          allColumnBucks.append(uniqueColumns.toArray)
//        }
//      }
//    }
//
//    val finalSelectedColumnArray = columnIndex -- selectedColumnArray   // 不满足相似度值的元素
//
//    // 提取元素(随机挑选一个)，判断len==1的条件可以省略
//    for(i <- allColumnBucks){
//      val len = i.length
//      finalSelectedColumnArray.append(i(Random.nextInt(len)))
//    }
//
//    finalSelectedColumnArray.map(_.toInt).toArray
//
//  }


//  def selectAttributes1(columnIndex:ArrayBuffer[Long], cosineSimilarities: RDD[MatrixEntry], similarityValue: Double):Array[Int] = {
//
//    val cosineValue: RDD[(Long, (Long, Double))] = cosineSimilarities.map(item => (item.i,(item.j,item.value)))
//    val groupCosineValue: RDD[(Long, Iterable[(Long, Double)])] = cosineValue.groupByKey()
//    val groupCosineValueArray: Array[(Long, Iterable[(Long, Double)])] = groupCosineValue.sortBy(_._1).collect()
//
//
//    // 保存所有根据余弦值聚集的列名信息
//    val allColumnBucks = ArrayBuffer.empty[Array[Long]]
//    // 保存已经选过的列名信息
//    val selectedColumnArray = ArrayBuffer.empty[Long]
//
//
//
//    for(i <- groupCosineValueArray){
//      // 每个key聚类中满足条件的列号
//      val elementArray = ArrayBuffer.empty[Long]
//      if(!selectedColumnArray.contains(i._1)){
//        elementArray.append(i._1)
//        for(j <- i._2){
//          if(j._2 >= similarityValue){
//            elementArray.append(j._1)
//          }
//        }
//        val uniqueColumns = elementArray -- selectedColumnArray
//
//        if(uniqueColumns.length > 1){
//          selectedColumnArray ++= uniqueColumns
//          allColumnBucks.append(uniqueColumns.toArray)
//        }
//      }
//    }
//
//    val finalSelectedColumnArray = columnIndex -- selectedColumnArray   // 不满足相似度值的元素
//
//    // 提取元素(随机挑选一个)，判断len==1的条件可以省略
//    for(i <- allColumnBucks){
//      val len = i.length
//      if(len == 1){
//        finalSelectedColumnArray.append(i(0))
//      }else{
//        finalSelectedColumnArray.append(i(Random.nextInt(len)))
//      }
//    }
//
//    finalSelectedColumnArray.map(_.toInt).toArray
//  }






  val discretize = (sc:SparkContext, rdd:RDD[Array[Double]], nbDiscretBucket:Int) =>
  {
    rdd.cache
    val minMaxArray = rdd.map(_.map(v => (v, v)) ).reduce( (v1, v2) => v1.zip(v2).map{ case(((min1,max1),(min2,max2))) => (min(min1, min2), max(max1, max2))})

    val bucketSize = minMaxArray.map{ case(min, max) => ((max - min) / nbDiscretBucket, min) }

    val ranges = for( (size, min) <- bucketSize ) yield( for( limit <- 0 until nbDiscretBucket )
      yield( min + size * limit ) )

    val discretizeRDDstr = rdd.map( vector => vector.zipWithIndex.map{
      case(value, idx) => (whichInterval(value, ranges(idx)).toString, idx) })

    val fromStrToIdx = sc.broadcast(
      ranges.map( range =>
        (range :+ Double.PositiveInfinity).map( v => whichInterval(v, range).toString ).zipWithIndex.map{ case(str, idx) => (str, idx.toDouble) }.toMap )
        .zipWithIndex
        .map(_.swap)
        .toMap
    )

    val discretizeRDD = discretizeRDDstr.map(_.map{ case(v, idx2) => fromStrToIdx.value(idx2)(v) })
    rdd.unpersist(false)
    discretizeRDD
  }


  /*
   * Determine in which interval falls a value given a specific range. Left exclude Right include
   */
  val whichInterval = (d:Double, range:IndexedSeq[Double]) =>
  {
    var continue = true
    var bucketNumber = 0
    while( continue && d > range(bucketNumber) )
    {
      bucketNumber += 1
      if( bucketNumber == range.size ) continue = false
    }
    if ( bucketNumber == 0 ) (Double.NegativeInfinity,range(bucketNumber))
    else if ( bucketNumber == range.size ) (range(bucketNumber - 1), Double.PositiveInfinity)
    else (range(bucketNumber - 1), range(bucketNumber))
  }



  val divideByFeatures = (readyForDimReduceRDD:RDD[(Array[Double], Double)], sizeColumns:Int, selectedAttributes:Array[Int]) => {

    val nbFeatures = selectedAttributes.length
    val nbColumns = nbFeatures / sizeColumns

    val shuffleFeats = Random.shuffle[Int,IndexedSeq](selectedAttributes).toArray

    val columnsOfFeats = if(nbFeatures % sizeColumns != 0){
      (for(i <- 0 to nbColumns) yield shuffleFeats.slice(sizeColumns * i, sizeColumns *(i + 1))).toArray
    }else{
      (for(i <- 0 until nbColumns) yield shuffleFeats.slice(sizeColumns * i, sizeColumns *(i + 1))).toArray
    }

    val divideByFeatsRDD: RDD[(Array[Array[Double]], Double)] = readyForDimReduceRDD.map{ case( vector, label) => ( for(feats <- columnsOfFeats) yield(for(feat <- feats) yield(vector(feat))), label)}
    (divideByFeatsRDD, columnsOfFeats)
  }


/*

  // 随机划分块
//  val divideByFeatures = (readyForDimReduceRDD:RDD[(Array[Double], Double)], nbColumns:Int, nbFeatures:Int) =>
  val divideByFeatures1 = (readyForDimReduceRDD:RDD[(Array[Double], Double)], nbColumns:Int, nbFeatures:Int, selectedAttributes:Array[Int]) =>
  {
    val sizeColumns = nbFeatures / nbColumns  // sizeColumns:the number of features in each splited data.  nbColumns :the number of  splited data


    // 无selectedAttributes
//    val shuffleFeats = Random.shuffle[Int,IndexedSeq](0 to nbFeatures - 1).toArray

    // selectedAttributes
    val shuffleFeats = selectedAttributes





    val columnsOfFeats: Array[Array[Int]] = if(nbColumns == 1) {
      Array((0 to nbFeatures-1).toArray)
    } else if (nbFeatures % nbColumns != 0){
      (for(i <- 0 to nbColumns  )
        yield (shuffleFeats.slice(sizeColumns * i, sizeColumns * (i+1)))).toArray
    }else{
      (for(i <- 0 until nbColumns  )
        yield (shuffleFeats.slice(sizeColumns * i, sizeColumns * (i+1)))).toArray
    }

    val divideByFeatsRDD = if( nbColumns == 1 ) {
      readyForDimReduceRDD.map{ case(vector, label) => (Array(vector), label) }
    } else readyForDimReduceRDD.map{ case( vector, label) => ( for( feats <- columnsOfFeats ) yield(for( feat <- feats ) yield(vector(feat))), label) }

    (divideByFeatsRDD, columnsOfFeats)

  }


 */

}
