# Customize these paths for your environment.
# -----------------------------------------------------------
hadoop.root=/Users/amogh-hadoop/Downloads/hadoop-2.7.3
pr.jar.name=PageRank_Hw5_2-1.0-jar-with-dependencies.jar
pr.jar.path=target/${pr.jar.name}
pr.job.name=PageRankHw5
local.input=input/wikipedia-simple-html.bz2
local.output=outputS
# Pseudo-Cluster Execution
hdfs.user.name=hsamogh
hdfs.input=input
hdfs.output=output
# AWS EMR Execution
aws.emr.release=emr-5.3.1
aws.region=us-east-1
aws.bucket.name=amogh-hw5
aws.subnet.id=subnet-447f3269
aws.input=input
aws.output=outputs
aws.log.dir=log
aws.num.nodes=5
aws.instance.type=m4.large
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean compile package

# Removes local output directory.
clean-local-output:
	rm -rf ${local.output}*

Remove-inf-files:
	zip -d ${pr.jar.path} META-INF/LICENSE

# Runs standalone
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
alone: jar clean-local-output Remove-inf-files
	${hadoop.root}/bin/hadoop jar ${pr.jar.path} ${job.name} ${local.input} ${local.output}

# Start HDFS
start-hdfs:
	${hadoop.root}/sbin/start-dfs.sh

# Stop HDFS
stop-hdfs:
	${hadoop.root}/sbin/stop-dfs.sh

# Start YARN
start-yarn: stop-yarn
	${hadoop.root}/sbin/start-yarn.sh

# Stop YARN
stop-yarn:
	${hadoop.root}/sbin/stop-yarn.sh

# Reformats & initializes HDFS.
format-hdfs: stop-hdfs
	rm -rf /tmp/hadoop*
	${hadoop.root}/bin/hdfs namenode -format

# Initializes user & input directories of HDFS.
init-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -rm -r -f /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}/${hdfs.input}

# Load data to HDFS
upload-input-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -put ${local.input}/* /user/${hdfs.user.name}/${hdfs.input}

# Removes hdfs output directory.
clean-hdfs-output:
	${hadoop.root}/bin/hdfs dfs -rm -r -f ${hdfs.output}*

# Download output from HDFS to local.
download-output:
	mkdir ${local.output}
	${hadoop.root}/bin/hdfs dfs -get ${hdfs.output}/* ${local.output}

# Create S3 bucket.
make-bucket:
	aws s3 mb s3://${aws.bucket.name}

# Upload data to S3 input dir.
upload-input-aws: make-bucket
	aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}

# Delete S3 output dir.
delete-output-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output}*"

# Upload application to S3 bucket.
upload-app-aws:
	#aws s3 cp ${combiner.jar.path} s3://${aws.bucket.name}/combiner/
	#aws s3 cp ${nocombiner.jar.path} s3://${aws.bucket.name}/simple/
	#aws s3 cp ${inmapcombiner.jar.path} s3://${aws.bucket.name}/inmapcombiner/
	aws s3 cp ${secondarysort.jar.path} s3://${aws.bucket.name}/secondarysort/


# Main EMR launch.
cloud-combiner:
		$(call run-cluster-1,"Combiner Cluster",combiner,${combiner.jar.name})
cloud-nocombiner:
		$(call run-cluster-1,”Simple Cluster”,WeatherDataSimple,${nocombiner.jar.name})
cloud-inmapcombiner:
		$(call run-cluster-1,"InMap Combiner Cluster",inmapcombiner,${inmapcombiner.jar.name})
cloud-secondarysort:
		$(call run-cluster-2,"SecondarySort Cluster",secondarysort,${secondarysort.jar.name})
define run-cluster-1
		aws emr create-cluster \
		--name $(1) \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["s3://${aws.bucket.name}/${aws.input}/1991.csv","s3://${aws.bucket.name}/$(2)/${aws.output}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/$(2)/$(3)","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/$(2)/${aws.log.dir} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
		--region ${aws.region} \
		--enable-debugging
		#--auto-terminate
endef
define run-cluster-2
		aws emr create-cluster \
		--name $(1) \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["s3://${aws.bucket.name}/${aws.input}/1880.csv","s3://${aws.bucket.name}/${aws.input}/1881.csv","s3://${aws.bucket.name}/${aws.input}/1882.csv","s3://${aws.bucket.name}/${aws.input}/1883.csv","s3://${aws.bucket.name}/${aws.input}/1884.csv","s3://${aws.bucket.name}/${aws.input}/1885.csv","s3://${aws.bucket.name}/${aws.input}/1886.csv","s3://${aws.bucket.name}/${aws.input}/1887.csv","s3://${aws.bucket.name}/${aws.input}/1888.csv","s3://${aws.bucket.name}/${aws.input}/1889.csv","s3://${aws.bucket.name}/$(2)/${aws.output}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/$(2)/$(3)","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/$(2)/${aws.log.dir} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
		--region ${aws.region} \
		--enable-debugging
		#--auto-terminate
endef

# Download output from S3.
download-output-aws: clean-local-output
	mkdir ${local.output}
	aws s3 sync s3://${aws.bucket.name}/${aws.output} ${local.output}

# Change to standalone mode.
switch-standalone:
	cp config/standalone/*.xml ${hadoop.root}/etc/hadoop

# Change to pseudo-cluster mode.
switch-pseudo:
	cp config/pseudo/*.xml ${hadoop.root}/etc/hadoop

# Package for release.
distro:
	rm -rf build
	mkdir build
	mkdir build/deliv
	mkdir build/deliv/WordCount
	cp pom.xml build/deliv/WordCount
	cp -r src build/deliv/WordCount
	cp Makefile build/deliv/WordCount
	cp README.txt build/deliv/WordCount
	tar -czf WordCount.tar.gz -C build/deliv WordCount
	cd build/deliv && zip -rq ../../WordCount.zip WordCount
