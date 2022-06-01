#!/bin/sh

isArgPassed() {
  arg="$1"
  argWithEqualSign="$1="
  shift
  while [ $# -gt 0 ]; do
    passedArg="$1"
    shift
    case $passedArg in
    $arg)
      return 0
      ;;
    $argWithEqualSign*)
      return 0
      ;;
    esac
  done
  return 1
}

case "$1" in

  'master')
  	ARGS="-mdir=/data -volumePreallocate -volumeSizeLimitMB=1024"
  	shift
  	exec /usr/bin/weed -logtostderr=true master $ARGS $@
	;;

  'volume')
  	ARGS="-dir=/data -max=0"
  	if isArgPassed "-max" "$@"; then
  	  ARGS="-dir=/data"
  	fi
  	shift
  	exec /usr/bin/weed -logtostderr=true volume $ARGS $@
	;;

  'server')
  	ARGS="-dir=/data -volume.max=0 -master.volumePreallocate -master.volumeSizeLimitMB=1024"
  	if isArgPassed "-volume.max" "$@"; then
  	  ARGS="-dir=/data -master.volumePreallocate -master.volumeSizeLimitMB=1024"
  	fi
 	shift
  	exec /usr/bin/weed -logtostderr=true server $ARGS $@
  	;;

  'filer')
  	ARGS=""
  	shift
  	exec /usr/bin/weed -logtostderr=true filer $ARGS $@
	;;

  's3')
  	ARGS="-domainName=$S3_DOMAIN_NAME -key.file=$S3_KEY_FILE -cert.file=$S3_CERT_FILE"
  	shift
  	exec /usr/bin/weed -logtostderr=true s3 $ARGS $@
	;;

  'shell')
  	ARGS="-cluster=$SHELL_CLUSTER -filer=$SHELL_FILER -filerGroup=$SHELL_FILER_GROUP -master=$SHELL_MASTER -options=$SHELL_OPTIONS"
  	shift
  	exec echo "$@" | /usr/bin/weed -logtostderr=true shell $ARGS
  ;;

  *)
  	exec /usr/bin/weed $@
	;;
esac
