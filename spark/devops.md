# DevOps/Cloud

#### *Module 1 : Les concepts de Cloud/Basiques* 
* Pourquoi les Services Cloud
* Types et Modèles de Cloud
* Types de services Cloud
* Présentation du IAAS/PAAS/SAAS/Application Service Provider
* Risques liés à la sécurité, privacy, RGPD, intégrité, ... tout ce qui est lié au frein à l'adoption du Cloud public 
* Atelier : Création de son compte chez AWS et Azure
* Atelier : Découverte des portails d’administration des clouders

#### Module 2 : IMPLÉMENTER DES SOLUTIONS QUI UTILISENT DES MACHINES VIRTUELLES
* Provisionnement de machines virtuelles
* Atelier : Créer et configurer son environnement de Dev dans une VM
* Outils d'automatisation de provisionning de VMs sur le Cloud (Ansible, TerraForm)
* Création d'une image suite à plusieurs manipulations: vm linux + tomcat jboss + base de données postgresql
    * Manuellement + automatique


#### Module 3 : CRÉATION DE SERVICES WEB
* Présentation des concepts PAAS 
* Création d’application Web + API Rest
* Atelier : Déploiement d’une application WEB en mode PAAS (haute dispo + load balancing + secu+scalabilité+...) avec Docker
* A la fin, on va avoir un docker file + dockercompose/kubernetes pour déployer les applications


#### Module 4 : DÉVELOPPEMENT DE SOLUTIONS UTILISANT UNE BASE DE DONNÉES RELATIONNELLE
* Présentation d’Azure SQL At AWS RDS
* Création et accès à une base de données
* Mise à jour et suppressions de tables à l’aide de code
* Atelier : Création d’un BDD en mode PAAS et connexion au site web PAAS
* Principe de Serverless
* Optimisation, deploiement automatique + monitoring

#### Module 5 : *IMPLÉMENTATION DE L’AUTHENTIFICATION*
* Présentation de la plateforme Microsoft Identity
* Implémentation de l’authentification OAuth2
* Implémentation des identités managées ???? C'est quoi ? Profils? Droits d'accès ?
* Atelier Implémentation de l’authentification AZURE AD / Aws IAAM dans une application web

#### Module 6 : *IMPLÉMENTATION DE LA SÉCURITÉ DES DONNÉES*
* Options de chiffrement
* Chiffrement de bout en bout
* Gestion des clés de chiffrement
* Atelier : Chiffrement des données

#### Chaine CI/CD
* Maven/Sbt
* Nexus
* Jenkins
* Git
* Qualité du code: SonarQube/PMD/Checkstyle/findBug
* XLDeploy (DeployIt)

#### Monitoring/Alerting/Astreintes
* Qu'elles sont les points à aborder: logs applicatifs, metrics, alerte, logs infras 

#### Agilité
* JIRA/JIRA Agile
* Scrum: s'inspirer de scrumble pour le TP. Le cours il y a les slides
* Kanban
* Confluence: Wiki (best practices: lien entre JIRA et Confluence)

#### *Infrastructure As a Code (IAC)*
* Présentation du concept
* Avantages/Inconvénients
* Dockerisation du monde du DEV
* Atelier: application web + scripts de deploiement intégrés
