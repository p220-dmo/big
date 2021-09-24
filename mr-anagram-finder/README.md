# Hadoop MapReduce WordCount 

Ce tutorial permet de comprendre le fonctionnement du framework MapReduce dans Hadoop

Merci de suivre les étapes suivantes :

### 0. Jeu de données 
Rendez-vous sur le site : http://www.gutenberg.org/catalog/

Il contient des livres gratuits, nous allons choisir un livre et télécharger sa version texte.

Transférez le/les livres vers HDFS

### 1. Clonez le projet en local
`git clone https://github.com/hbellahc/mapreduce.git`

### 2. Buildez le projet en local 
`mvn clean install` ou un build classique avec votre IDE préféré

### 3. Transférez le jar sur la machine sandbox
Vous avez plusieurs possibilités pour effectuer cette action :

#### Via un client SCP (winscp, filezilla etc)
#### En ligne de commande 
Placez-vous dans le répertoire ou se trouve le jar que l'on souhaite transférer et exécutez la commande suivante :
 
`stp -P 2222 <chemin local> root@sandbox-hdp.hortonworks.com:<chemin dans la sandbox>`

### 4. Exécutez le job MapReduce sur la SandBox 
#### Connectez-vous  en ssh 
`ssh root@sandbox-hdp.hortonworks.com`
#### Placez-vous dans le répértoire ou se trouve votre jar
`cd /home/hdfs/`
#### Lancez le job
`hadoop jar mapreduce-1.0-SNAPSHOT.jar WordCount /user/hdfs /user/hdfs/out`
#### Monitorez le job 
Dans les logs vous allez voir une ligne que vous donne l'url pour monitorer votre job
18/05/11 08:54:14 INFO mapreduce.Job: The url to track the job: **http://sandbox-hdp.hortonworks.com:8088/proxy/application_1526028785803_0001/**



