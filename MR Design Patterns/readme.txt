// Amogh Huilgol
// CS6240
// Section 02

The assignment has the following contents
--> Assignment Report (Assignment2_Report.pdf)
--> Deliverables for No-combiner approach (WeatherSimple folder)
--> Deliverables for Combiner approach (WeatherDataCombiner folder)
--> Deliverables for In-Mapper Combining approach (WeatherDataInMapper folder)
--> Deliverables for Secondary Sort approach (WeatherSecondarySort folder)
--> Make-file : used to run code
--> The code picks the data from input folder. the makefile is configured to
    pick inputs from input folder

MakeFile
----------
Place the make file in the HW2 directory. The input data needs to be placed in
input folder. Following are the commands that can be used to run a makefile. The
make file needs to be configured according to the programming environment. Following
are the fields that need to be changed in make file

hadoop.root : stores the path of hadoop distribution
hdfs.user.name: <username of hdfs account>
hdfs.input: <input location in  hdfs>
hdfs.output: <output location of hdfs>
aws.bucket.name: <bucket name  in S3>
aws.subnet.id: <subnet-id in aws>
aws.input: <aws input>
aws.output: <aws output>
aws.log.dir: <Location of log directory>

Following are the commands that can be run on Makefile


make alone-simple : Runs the no-combiner program

make alone-combiner : runs the combiner approach program

make-alone-inmapper : runs im-mapper combining program

make-alone-ssort : runs secondary sort program


Each of the folder contains following contents

WeatherSimple
-----------
Cloud-Output : contains outputs of running the program on cloud-simple
controller-1.txt, controller2.txt : consists of log details of two executions on AWS
syslog-1.txt , sys-log2.txt : consists of log details of two executions on AWS
pom.xml : needed for making jar files
src : Consists of source code . Used by make files

For convenience a copy of source code is also placed outside in the WeatherSimple
folder


WeatherDataCombiner
-----------
Cloud-Output : contains outputs of running the program on cloud-simple
controller-1.txt, controller2.txt : consists of log details of two executions on AWS
syslog-1.txt , sys-log2.txt : consists of log details of two executions on AWS
pom.xml : needed for making jar files
src : Consists of source code . Used by make files

For convenience a copy of source code is also placed outside in the WeatherDataCombiner
folder


WeatherDataInMapper
-----------
Cloud-Output : contains outputs of running the program on cloud-simple
controller-1.txt, controller2.txt : consists of log details of two executions on AWS
syslog-1.txt , sys-log2.txt : consists of log details of two executions on AWS
pom.xml : needed for making jar files
src : Consists of source code . Used by make files

For convenience a copy of source code is also placed outside in the WeatherDataCombiner
folder

WeatherSecondarySort
-----------
Cloud-Output : contains outputs of running the program on cloud-simple
controller.txt : consists of log details of  execution on AWS
syslog.txt  : consists of log details of  execution on AWS
pom.xml : needed for making jar files
src : Consists of source code . Used by make files

For convenience a copy of source code is also placed outside in the WeatherDataCombiner
folder
