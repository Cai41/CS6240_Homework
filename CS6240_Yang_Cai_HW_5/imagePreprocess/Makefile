# Makefile for MapReduce Page Rank project.

# Customize these paths for your environment.
# -----------------------------------------------------------
jar.name=imagePreprocess-1.0-jar-with-dependencies.jar
jar.path=target/${jar.name}
job.name=cs6240.imagePreprocess.Preprocess
local.uri=localhost
local.input=input
local.output.result=output_result
local.output.image=output_image
local.xx=3
local.yy=3
local.zz=3
local.sample.size=12500
# AWS EMR Execution
aws.emr.release=emr-5.8.0
aws.region=us-east-1
aws.bucket.name=cs6240-yang-cai
aws.subnet.id=subnet-9b31f4a4
aws.input=input
aws.output.result=output_result
aws.output.image=output_image
aws.xx=21
aws.yy=21
aws.zz=7
aws.sample.size=12500
aws.log.dir=log
aws.num.nodes=5
aws.instance.type=m4.large
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean package

# Removes local output directory.
clean-local-output:
	rm -rf ${local.output.result}
	rm -rf ${local.output.image}

# Runs standalone
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
alone: jar clean-local-output
	spark-submit --class ${job.name} --master local[2] ${jar.path} ${local.uri} ${local.input} ${local.output.result} ${local.output.image} \
	${local.xx} ${local.yy} ${local.zz} ${local.sample.size}

# Create S3 bucket.
make-bucket:
	aws s3 mb s3://${aws.bucket.name}

# Upload data to S3 input dir.
upload-input-aws: make-bucket
	aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}

# Delete S3 output dir.
delete-output-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output.result}" --include "${aws.output.image}"

# Upload application to S3 bucket.
upload-app-aws:
	aws s3 cp ${jar.path} s3://${aws.bucket.name}

# Main EMR launch.
cloud: jar upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "Image Preprocess" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Spark \
	    --steps '[{"Args":["spark-submit","--deploy-mode","cluster","--executor-cores", "2", "--class", "${job.name}", "--master","yarn", "s3://${aws.bucket.name}/${jar.name}", "s3://${aws.bucket.name}/", "s3://${aws.bucket.name}/${aws.input}","s3n://${aws.bucket.name}/${aws.output.result}", "s3n://${aws.bucket.name}/${aws.output.image}", "${aws.xx}", "${aws.yy}", "${aws.zz}", "${aws.sample.size}"],"Type":"CUSTOM_JAR","Jar":"command-runner.jar","ActionOnFailure":"CONTINUE","Name":"Spark application"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
		--region ${aws.region} \
		--enable-debugging \
		--auto-terminate
		
# Download output from S3.
download-output-aws: clean-local-output
	aws s3 sync s3://${aws.bucket.name}/${aws.output.result} ${local.output.result}
	aws s3 sync s3://${aws.bucket.name}/${aws.output.image} ${local.output.image}
