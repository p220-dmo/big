* TO Global
   * Créer une branche de solution par step pour les TPs
* TO pour Spark Core RDD
  * reduce(func)
  * ~~broadcast~~
  * accumulator
  * ~~collect()~~
  * ~~collectAsMap()~~
  * count()
  * first()
  * ~~take(n)~~
  * ~~saveAsTextFile(path)/saveAsSequenceFile(path)/saveAsObjectFile(path)~~
  * ~~countByKey()~~
  * ~~map(fun)~~
  * ~~mapToPair(func)~~
  * flatMap(func)
  * flatMapToPair(func)
  * ~~filter(func)~~
  * groupBy()
  * ~~groupByKey()~~
  * ~~reduceByKey(func)~~
  * sortByKey([ascending])
  * ~~join(otherDataset)~~
  * intersect(otherDataset)
  * distinct()
  * union(otherDataset)
  * cogroup
  * ~~mapPartition: bonnes explications sur http://bytepadding.com/big-data/spark/spark-mappartitions/~~
     * ~~Utilisé généralement quant on utilise un service externe~~
  * ~~forEach~~
  * ~~foreachPartition~~
* TODO pour Spark DataFrame:
    * Refaire les même exercice avec DataFrame
    * Ajouter les passage entre DataFrame vers JavaRDD
    * Ajouter la création de table requêtable avec SQL (local et global)
    * Ajouter la construction de cubes
    * Ajouter un exemple avec aggrégation (agg)
   
* TODO pour Spark DataSet
    * Refaire les même exercice avec DataSet
    * Ajouter les passage entre DataSet vers JavaRDD
    * Ajouter le passage vers DataFrame et vice versa
    * Ajouter un exemple avec aggrégation (agg)
    * Ajouter la création de table requêtable avec SQL (local et global)
 
 * TODO pour Spark Streaming (TO BE DEFINED)
   * Considérer le use case ou on joint un dataframe chargé depuis HDFS et un stream venant de kafka (ça permet de répondre au use case Booper où ils mettent à jour des données chargées pour recalculer les prix)
 * TODO pour Spark Structured Streaming (TO BE DEFINED)

