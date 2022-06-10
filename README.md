## Description
Ceci est le repo git pour le projet SPARK de SCIA 2023 de:
- Richard LAY (richard.lay)
- Jacky WU (jacky.wu)
- Steven Tien (steven.tien)


Ce projet est un programme scalable, fault tolerant écrit en scala.</br></br>
Le projet suit le fonctionnement des alertes à Peaceland.</br></br>
Les drones envoient de la donnée en continue dans une stream, cette donnée va ensuite être extraite pour envoyer des alertes si nécessaire puis stocker dans un datalake Azure.</br>
</br>
La donnée va ensuite être analysée afin d'y extraire des comportements dangereux au sein de Peaceland.


Il est composé de 5 sous-projets:
- drone
- message_storing
- uploadToDatalake
- alert
- data_processing

## Requirement 
```
Sbt 1.6.2
Scala 2.13.8
Java 1.8/11
Kafka 2.13-3.2.0 
```
## Utilisation
1. Lancer le service Kafka
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```
2. Puis dans un nouveau terminal
```
bin/kafka-server-start.sh config/server.properties
```
3. Lancer le projet messageStoring
```
## Dans le dossier messageStoring (src/message_storing)
sbt run
```
4. Lancer le projet UploadToDatalake
```
## Dans le dossier (src/UploadToDatalake)
sbt run
```
5. Lancer le projet alert
```
## Dans le dossier (src/alert)
sbt run
```
6. Lancer le projet drone
```
## Dans le dossier (src/drone)
sbt run
```
7. Lancer le projet data_processing
```
## Dans le dossier (src/data_processing)
sbt run
```
