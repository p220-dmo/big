# Spark Streaming

## Introduction
La description des données utilisées pour ses exercices est accessible [ici](https://github.com/Ahmed-Gater/spark-in-practice/blob/master/datasetdescription.md).
La version de Spark pour ces exercices est 2.4.2 et scala 11.  

## Exercices
<details><summary>Exercice 1: Quel est le point d'entrée pour un Job Spark Streaming?</summary>
<p>

#### C'est JavaStreamingContext !!!
```
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("Spark Streaming training");
JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
```
</p>
</details>
