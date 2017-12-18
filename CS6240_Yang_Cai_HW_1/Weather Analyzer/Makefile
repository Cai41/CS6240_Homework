# Makefile for MapReduce Page Rank project.

# Customize these paths for your environment.
# -----------------------------------------------------------
jar.name=WeatherAnalyzer-0.0.1-SNAPSHOT.jar
jar.path=target/${jar.name}
path=test.csv
fibn=17

# Compiles code and builds jar
jar:
	mvn clean package
	
run-seq:
	java -jar ${jar.path} seq ${path} ${fibn}
	
run-nolock:
	java -jar ${jar.path} nolock ${path} ${fibn}
	
run-coarse:
	java -jar ${jar.path} coarselock ${path} ${fibn}
	
run-fine:
	java -jar ${jar.path} finelock ${path} ${fibn}
	
run-noshare:
	java -jar ${jar.path} noshare ${path} ${fibn}

run-all: run-nolock run-coarse run-seq run-fine run-noshare 

# Package for release.
distro:
	rm -rf build
	mkdir -p build/deliv/WeatherAnalyzer
	cp pom.xml build/deliv/WeatherAnalyzer
	cp -r src build/deliv/WeatherAnalyzer
	cp Makefile build/deliv/WeatherAnalyzer
	cp README.txt build/deliv/WeatherAnalyzer
	tar -czf WeatherAnalyzer.tar.gz -C build/deliv WeatherAnalyzer
	cd build/deliv && zip -rq ../../WeatherAnalyzer.zip WeatherAnalyzer
	
