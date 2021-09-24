# Hadoop MapReduce WordCount 
### Analyse du code 
Il y'a trois éléments importants à considérer dans un job MapReduce :
1. Le code du Mapper
2. Le code du Reducer 
3. Le code du Job (son lancement)

### WordCount 
Le wordcount consiste à compter combien de fois apparait un mot dans un texte.

#### 1. Définition de la clé 
La définition de la clé est une étape importante dans la conception d'un job MapReduce, ici nous souhaitons calculer combien de fois un mot apparait,
le mot lui-même semble donc une clé pertinente.

#### 2. Mapper 

`public void map(Object key, Text value, Context context) {}`

Le mapper prend en paramètres une ligne du texte et la découpe en plusieurs mots et il retourne un couple clé valeur (le mot, 1)

Le découpage se fait sur l'espace, retour à la ligne, tabulations.

En Java le Mapper prend en paramètres un **objet clé**, le **texte** à traiter, le **contexte**

1. La **clé** : la clé ici ne sera pas utilisée
2. Le **texte** : c'est la ligne à traiter (celle qui sera découpée)
3. Le **contexte** : sera utilisé pour y insérer les résultats du découpage (mapping)

#### 3. Reducer   
  
`public void reduce(Text key, Iterable<IntWritable> values,Context context){}`


Le reducer reçoit une **clé** et l’ensemble des **valeurs** de cette **clé** et fait-en sorte de calculer la somme des valeurs.

En Java le Reducer prend en paramètres un **objet clé**, la liste des **valeurs** pour la **clé** et le **contexte**

#### 4. Job

##### Configuration du job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

##### Définition de la classe java
        job.setJarByClass(WordCount.class);

##### Définition du mapper
        job.setMapperClass(TokenizerMapper.class);

##### Définition du Combiner et Reducer
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

##### Définition des classes java pour les output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

##### Définition des chemins des fichiers hdfs en entrée et en sortie

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

##### Lancement du JOB
        System.exit(job.waitForCompletion(true) ? 0 : 1);  

