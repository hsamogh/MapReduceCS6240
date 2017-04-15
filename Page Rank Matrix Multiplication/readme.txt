// Amogh Huilgol
// CS6240
// Section 02

The assignment has the following contents
--> Assignment Report (ASSIGNMENT5_REPORT.pdf)
--> folder containing cloud deliverables(Cloud_output)
—-> pom.xml : used to run code. Used my make-file
—-> MakeFile : Used to run code


Running Instructions
————————————————————

The input data needs to be placed in input folder. Following are the commands that can be used to run a makefile. The make file needs to be configured according to the programming environment. Following are the fields that need to be changed in make file

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



make alone : Runs local version of program

Note : In case of an cache accessibilty exception , place the project in /tmp folder
and rerun the project
