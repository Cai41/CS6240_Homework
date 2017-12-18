jar.name=WeatherScala-1.0-jar-with-dependencies.jar
job.name.1=CS6240.WeatherScala.Analyze
job.name.2=CS6240.WeatherScala.AnalyzeSecondarySort
jar.path=target/${jar.name}
no.combiner=NoCombiner
with.combiner=WithCombiner
inmapper.combiner=InMapperCombiner
local.input=input
local.output=output

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean package
	
# Removes local output directory.
clean-local-output:
	rm -rf ${local.output}*
	
nocombiner: jar clean-local-output
	java -cp ${jar.path} ${job.name.1} ${no.combiner} ${local.input} ${local.output}
	
withcombiner: jar clean-local-output
	java -cp ${jar.path} ${job.name.1} ${with.combiner} ${local.input} ${local.output}
	
inmappercombiner: jar clean-local-output
	java -cp ${jar.path} ${job.name.1} ${inmapper.combiner} ${local.input} ${local.output}
	
secondarysort: jar clean-local-output
	java -cp ${jar.path} ${job.name.2} ${local.input} ${local.output}
	