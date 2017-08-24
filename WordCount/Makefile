# Rodrigo Noronha Máximo - 157209 - 22/08/2017

# Makefile to compilar e executar o wordcount no pseudo-distributed file system using hadoop.

run: compile 
	# REMOVING CURRENT OUTPUT
	rm -rf ~/Documents/Unicamp/8-Semestre/MC855/WordCount/output
	# RUNNING THE APPLICATION
	/usr/local/bin/hadoop jar wc.jar WordCount input output

compile:
	# Compilar java e criar o jar
	/usr/local/bin/hadoop com.sun.tools.javac.Main WordCount.java
	jar cf wc.jar WordCount*.class