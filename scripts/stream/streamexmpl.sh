#!/bin/sh

exampleHome=$(cd $(dirname $0);pwd)
exampleConfigFile=${exampleHome}/streamexmpl.properties
exampleLib=$(ls -tr ${exampleHome}/target/spark-kinetica-*-jar-with-dependencies.jar 2>/dev/null | tail -1)
exampleTestLib=$(ls -tr ${exampleHome}/target/spark-*-tests.jar 2>/dev/null | tail -1)

printf ${exampleHome}"\n"
printf ${exampleConfigFile}"\n"
printf ${exampleLib}"\n"

sparkPort=7077


function usage
{
	printf "Usage:  %s -t <example_type> -h <spark_host> [-p <spark_port>] [-f <data_file>]\n" $(basename $0)
	printf "Where:\n"
	printf "        <example_type> - of the following:\n"
	printf "            batch - to run RDD batch processing (not supported)\n"
	printf "            stream - to run streaming processing\n"
    printf "            dataFrame - to run data frame processing -requires data file (not supported)\n"
	printf "        <spark_host> - hostname/IP of the Spark master server\n"
	printf "        <spart_port> - port on which the Spark service is running\n"
    printf "        <data_file> - data file (if required)\n"
	exit 1
}


#########
#       #
# Batch #
#       #
#########

function batchExample
{
	printf " This will fail....."
}

##########
#        #
# Stream #
#        #
##########

function streamExample
{
	$SPARK_HOME/bin/spark-submit \
	        --class "com.kinetica.spark.StreamExample" \
	        --master ${sparkUrl} \
	        --driver-class-path "${exampleHome}" \
	        --jars "${exampleTestLib}" \
		--verbose \
	        ${exampleLib}
}

#############
#           #
# DataFrame #
#           #
#############

function dataFrameExample
{
	printf " This will fail....."
}


# Get options
while getopts t:h:p:f:x flag
do
	case ${flag} in
		t)
			runType=${OPTARG}
			;;
		h)
			sparkHost="${OPTARG}"
			;;
		p)
			sparkPort="${OPTARG}"
			;;
        f)
            dataFile="${OPTARG}"
            ;;
		*)
			usage
			exit 1
			;;
	esac
done
shift $((OPTIND-1))

sparkUrl=spark://${sparkHost}:${sparkPort}

[ -z "${sparkHost}" ] && usage

if [ -z "${SPARK_HOME}" ]
then
	printf "[ERROR] SPARK_HOME environment variable not set\n"
	exit 2
fi

if [ -z "${exampleLib}" ]
then
	printf "[ERROR] No application library found under <%s>\n" ${exampleHome}
	exit 2
fi

if [ ! -f "${exampleConfigFile}" ]
then
	printf "[ERROR] No application configuration file found at <%s>\n" ${exampleHome}
	exit 2
fi

case "${runType}" in
	batch)
		batchExample
		;;
	stream)
		streamExample
		;;
    dataFrame)
        dataFrameExample
        ;;
	*)
		usage
		;;
esac

exit 0
