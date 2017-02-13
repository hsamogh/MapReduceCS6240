# Customize these paths for your environment.
# -----------------------------------------------------------
hadoop.root=/Users/amogh-hadoop/Downloads/hadoop-2.7.3
simple.jar.name = WeatherDataSimple-1.0-jar-with-dependencies.jar
combiner.jar.name = WeatherDataCombiner-1.0-jar-with-dependencies.jar
imcombiner.jar.name = weatherDataInMapper-1.0-jar-with-dependencies.jar
ssort.jar.name = weatherSecondarySort-1.0-jar-with-dependencies.jar
simple.jar.path = target/${simple.jar.name}
combiner.jar.path = target/${combiner.jar.name}
inmapper.jar.path = target/${imcombiner.jar.name}
ssort.jar.path = target/${ssort.jar.name}
inmc.job.name=inMapperCombining
local.input=input/1991.csv
local.simple.output=simpleOutput
local.combiner.output=combinerOutput
local.inmapper.output=inmapperOutput
local.ssort.output = ssortOuput
# Pseudo-Cluster Execution
hdfs.user.name=hsamogh
hdfs.input=input
hdfs.output=output
# AWS EMR Execution
aws.emr.release=emr-5.3.1
aws.region=us-east-1
aws.bucket.name=amogh-hw2
aws.subnet.id=subnet-447f3269
aws.input=input
aws.output=outputs
aws.log.dir=log
aws.num.nodes=5
aws.instance.type=m4.large
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar-simple:
	(cd WeatherSimple && mvn clean compile package && cp ${simple.jar.path} ../${simple.jar.name})

# Compiles code and builds jar (with dependencies).
jar-combiner:
	(cd WeatherDataCombiner && mvn clean compile package && cp ${combiner.jar.path} ../${combiner.jar.name})

# Compiles code and builds jar (with dependencies).
jar-inmapper:
	(cd WeatherDataInMapper && mvn clean compile package && cp ${inmapper.jar.path} ../${imcombiner.jar.name})

# Compiles code and builds jar (with dependencies).
jar-ssort:
	(cd WeatherSecondarySort && mvn clean compile package && cp ${ssort.jar.path} ../${ssort.jar.name})

# Removes local output directory.
clean-local-output-simple:
	rm -rf ${local.simple.output}*

clean-local-output-combiner:
	rm -rf ${local.combiner.output}*

clean-local-output-inmapper:
	rm -rf ${local.inmapper.output}*

clean-local-output-ssort:
		rm -rf ${local.ssort.output}*

Remove-inf-files-simple:
	zip -d ${simple.jar.name} META-INF/LICENSE

Remove-inf-files-combiner:
	zip -d ${combiner.jar.name} META-INF/LICENSE

Remove-inf-files-inmapper:
	zip -d ${imcombiner.jar.name} META-INF/LICENSE

Remove-inf-files-ssort:
	zip -d ${ssort.jar.name} META-INF/LICENSE

# Runs standalone
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
alone-combiner: jar-combiner clean-local-output-combiner  Remove-inf-files-combiner
	${hadoop.root}/bin/hadoop jar ${combiner.jar.name} ${job.name} ${local.input} ${local.combiner.output}

# Runs standalone
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
alone-simple:  jar-simple clean-local-output-simple Remove-inf-files-simple
	${hadoop.root}/bin/hadoop jar ${simple.jar.name} ${job.name} ${local.input} ${local.simple.output}

# Runs standalone
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
alone-in-mapper:  jar-inmapper clean-local-output-inmapper Remove-inf-files-inmapper
	${hadoop.root}/bin/hadoop jar ${imcombiner.jar.name} ${job.name} ${local.input} ${local.inmapper.output}

# Runs standalone
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
alone-ssort: jar-ssort clean-local-output-ssort Remove-inf-files-ssort
	${hadoop.root}/bin/hadoop jar ${ssort.jar.name} ${job.name} ${local.input} ${local.ssort.output}


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
	aws s3 cp ${combiner.jar.path} s3://${aws.bucket.name}/combiner/
	aws s3 cp ${simple.jar.path} s3://${aws.bucket.name}/simple/
	aws s3 cp ${inmapper.jar.path} s3://${aws.bucket.name}/inmapcombiner/
	aws s3 cp ${ssort.jar.path} s3://${aws.bucket.name}/secondarysort/


# Main EMR launch.
cloud-combiner:
		$(call run-cluster-1,"Combiner",combiner,${combiner.jar.name})
cloud-simple:
		$(call run-cluster-1,”Simple”,WeatherDataSimple,${simple.jar.name})
cloud-inmapcombiner:
		$(call run-cluster-1,"In-Map Combiner",inmapcombiner,${imcombiner.jar.name})
cloud-secondarysort:
		$(call run-cluster-2,"SecondarySort",secondarysort,${ssort.jar.name})
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
