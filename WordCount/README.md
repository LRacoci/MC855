Rodrigo Noronha Máximo - 157209 - 22/08/2017

# Tutorial para rodar wordcount no HDFS

#### Exportar essas variáveis antes de rodar o Makefile:

- ```export JAVA_HOME=$(/usr/libexec/java_home)```
- ```export PATH=${JAVA_HOME}/bin:${PATH}```
- ```export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar```

___

## Pseudo-Distributed Operation - Single node

#### Diretórios do tutorial => Diretórios no macbook
- ```sbin/``` => ```/usr/local/opt/hadoop/libexec/sbin/```
- ```bin``` => ```/usr/local/bin/```

___

### Comandos úteis:

#### Remover diretório do hadoop cluster:

- ```/usr/local/bin/hdfs dfs -rm -r <diretorio>``` 

#### Visualizar diretorios e arquivos:

- ```/usr/local/bin/hdfs dfs -ls```

___ 

### Pseudo-Distributed Operation

Em ```/usr/local/opt/hadoop/libexec/etc/hadoop/``` alterar os arquivos:

- **```core-site.xml```**:
	
	```
	<configuration>
	    <property>
	        <name>fs.defaultFS</name>
	        <value>hdfs://localhost:9000</value>
	    </property>
	</configuration>```

- **```hdfs-site.xml```**:

	```
	<configuration>
	    <property>
	        <name>dfs.replication</name>
	        <value>1</value>
	    </property>
	</configuration>```

Feito isso, basta seguir o restante dos passos abaixo.

### 1) Ssh to the localhost:

- ```ssh localhost```

### 2) Format the filesystem: (NÃO PRECISA DENOVO)

- ```/usr/local/bin/hdfs namenode -format``` 

### 3) Start NameNode daemon and DataNode daemon:

- ```/usr/local/opt/hadoop/libexec/sbin/start-dfs.sh``` 

### 4) Verificar se http://localhost:50070/ está funcionando

- http://localhost:50070/ no navegador

### 5) Make the HDFS directories required to execute MapReduce jobs (NÃO PRECISA DENOVO)

- ```/usr/local/bin/hdfs dfs -mkdir /user```
- ```/usr/local/bin/hdfs dfs -mkdir /user/<username>```

### 6) Copy the input files into the distributed filesystem:

- ```/usr/local/bin/hdfs dfs -put <input> <inputHadoop>```

```<input>```: diretório que contém os arquivos que terão as palavras contadas.

```<inputHadoop>```: diretório de entrada que estará no hadoop.

### 7) Run some of the examples provided:

- ```/usr/local/bin/hadoop jar wc.jar WordCount <inputHadoop> <outputHadoop>```

```<inputHadoop>```: diretório de entrada que estará no hadoop, mesmo do passo 5.

```<outputHadoop>```: diretório de saída que estará no hadoop.

### 8) Ver output files no dfs:

- ```/usr/local/bin/hdfs dfs -cat output/part-r-00000```

### 9) Stop the daemons with:

- ```/usr/local/opt/hadoop/libexec/sbin/stop-dfs.sh```


