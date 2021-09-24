# Hadoop MapReduce Recherche d'anagrammes 
### AnagramFinder 

Le AnagramFinder consiste à chercher les mots qui sont composés par les mêmes lettres

Exemple : escorta,atroces,coteras,socrate,croates


#### 0. Jeu de données 
Nous allons utiliser un fichier contenant les mots du dictionnaire d'une langue donnée

Rendez-vous sur le site http://www.gwicks.net/dictionaries.htm et téléchargez le ou les dictionnaires qui vous conviennent.

Envoyez le/les fichiers vers HDFS

#### 1. Définition de la clé  

Quelle clé allons-nous utiliser ?

#### 2. Mapper 

`public void map(Object key, Text value, Context context) {}`

Le mapper prend en paramètres une ligne du texte et la découpe en plusieurs mots et 
Il retourne un couple clé valeur (la clé que vous avez défini et l'anagramme)


#### 3. Reducer   
  
`public void reduce(Text key, Iterable<Text> values, Context context)  {}`


Le reducer recevra une **clé** et l’ensemble des **valeurs** de cette **clé** et fera en sorte de **concaténer** les anagrammes.

Dans notre exemple, il devra donné cela :

**la clé** -> escorta,atroces,coteras,socrate,croates

#### 4. Job

On reprend la même chose que dans Le WordCount

