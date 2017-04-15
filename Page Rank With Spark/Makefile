run:
	sbt "run input output"
clean:
	sbt clean
# Customize these paths for your environment.
# -----------------------------------------------------------
spark.root=/Users/amogh-hadoop/spark
job.name=pageRank
pagerank.jar.name=pagerank_hw4_2.11-1.0.jar
pagerank.jar.path=target/scala-2.11/${pagerank.jar.name}
pagerank.job.name=pageRank
local.input=input
local.output=output
# Pseudo-Cluster Execution
hdfs.user.name=hsamogh
hdfs.input=input
hdfs.output=output
# AWS EMR Execution
aws.emr.release=emr-5.3.1
aws.region=us-east-1
aws.bucket.name=neu.mr.hw4
aws.subnet.id=subnet-447f3269
aws.input=input
aws.output=outputs
aws.log.dir=log
aws.num.nodes=10
aws.instance.type=m4.large
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	sbt package

# Removes local output directory.
clean-local-output:
	rm -rf ${local.output}*

# Upload application to S3 bucket.
upload-app-aws:
	aws s3 cp ${pagerank.jarA.path} s3://${aws.bucket.name}/pagerank/

alone: jar
	${spark.root}/bin/spark-submit --class ${job.name} --master local[*] ${pagerank.jar.path} ${local.input} ${local.output}

# Create S3 bucket.
make-bucket:
    aws s3 mb s3://${aws.bucket.name}

# Upload data to S3 input dir.
upload-input-aws: make-bucket
    aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}

# Delete S3 output dir.
delete-output-aws:
    aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output}*"

# Main EMR launch.
cloud:
	aws emr create-cluster \
	--name "HW4 PageRankCluster" \
	--release-label ${aws.emr.release} \
	--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	--applications Name=Spark \
	--steps '[{"Name":"Spark Program", "Args":["--class", "${job.name}", "--master", "yarn", "--deploy-mode", "cluster", "s3://${aws.bucket.name}/pagerank/${pagerank.jar.name}", "s3://neu.mr.hw3/${aws.input}","s3://${aws.bucket.name}/${aws.output}"],"Type":"Spark","Jar":"s3://${aws.bucket.name}/pagerank/${pagerank.jar.name}","ActionOnFailure":"TERMINATE_CLUSTER"}]' \
	--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
	--service-role EMR_DefaultRole \
	--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
	--configurations '[{"Classification":"spark", "Properties":{"maximizeResourceAllocation": "true"}}]' \
	--region ${aws.region} \
	--enable-debugging \
	--auto-terminate


#--steps '[{"Name":"Spark Program", "Args":["--class", "${job.name}", "--master", "yarn", "--deploy-mode", "cluster", "s3://${aws.bucket.name}/${jar.name}", "s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}"],"Type":"Spark","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER"}]' \
