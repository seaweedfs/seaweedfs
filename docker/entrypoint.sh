#!/bin/sh

# Fix permissions for mounted volumes
# If /data is mounted from host, it might have different ownership
# Fix this by ensuring seaweed user owns the directory
if [ "$(id -u)" = "0" ]; then
  # Running as root, check and fix permissions if needed
  SEAWEED_UID=$(id -u seaweed)
  SEAWEED_GID=$(id -g seaweed)
  DATA_UID=$(stat -c '%u' /data 2>/dev/null)
  DATA_GID=$(stat -c '%g' /data 2>/dev/null)
  
  # Only run chown -R if ownership doesn't match (much faster for subsequent starts)
  if [ "$DATA_UID" != "$SEAWEED_UID" ] || [ "$DATA_GID" != "$SEAWEED_GID" ]; then
    echo "Fixing /data ownership for seaweed user (uid=$SEAWEED_UID, gid=$SEAWEED_GID)"
    if ! chown -R seaweed:seaweed /data 2>&1; then
      echo "Warning: Failed to change ownership of /data. This may cause permission errors."
      echo "If /data is read-only or has mount issues, the application may fail to start."
    fi
  fi
  
  # Use su-exec to drop privileges and run as seaweed user
  exec su-exec seaweed "$0" "$@"
fi

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
