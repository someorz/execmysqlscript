#!/bin/bash
BASE_PATH=$(dirname $0)
java -Dlogback.configurationFile=${BASE_PATH}/../logback.xml -Dlog.dir=${BASE_PATH}/../logs -jar ${BASE_PATH}/../execmysqlscript-1.0-SNAPSHOT.jar
