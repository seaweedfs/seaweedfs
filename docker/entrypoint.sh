#!/bin/sh

WEED=${WEED-"/usr/bin/weed"}
WEED_DIR=${WEED_DIR-"/data"}
WEED_VOLUME_PREALLOCATE=${WEED_VOLUME_PREALLOCATE-"-volumePreallocate"}
WEED_MASTER_VOLUME_PREALLOCATE=${WEED_MASTER_VOLUME_PREALLOCATE-"-master.volumePreallocate"}
WEED_VOLUME_SIZE_LIMIT_MB=${WEED_VOLUME_SIZE_LIMIT_MB-1024}
WEED_MAX=${WEED_MAX-0}
WEED_S3_DOMAIN_NAME=${WEED_S3_DOMAIN_NAME-""}
WEED_S3_KEY_FILE=${WEED_S3_KEY_FILE-""}
WEED_S3_CERT_FILE=${WEED_S3_CERT_FILE-""}

WEED_MASTER=${WEED_MASTER-"localhost:9333"}
WEED_FILER=${WEED_FILER-"localhost:8888"}
FIX_REPLICATION_CRON_SCHEDULE=${FIX_REPLICATION_CRON_SCHEDULE-"*/7 * * * * *"}
BALANCING_CRON_SCHEDULE=${BALANCING_CRON_SCHEDULE-"25 * * * * *"}

case "$1" in
  'master')
    ARGS="-mdir=$WEED_DIR $WEED_VOLUME_PREALLOCATE -volumeSizeLimitMB=$WEED_VOLUME_SIZE_LIMIT_MB"
    shift
    exec $WEED master $ARGS $@
    ;;
  'volume')
    ARGS="-dir=$WEED_DIR -max=$WEED_MAX"
    shift
    exec $WEED volume $ARGS $@
    ;;
  'server')
    ARGS="-dir=$WEED_DIR -volume.max=$WEED_MAX $WEED_MASTER_VOLUME_PREALLOCATE -master.volumeSizeLimitMB=$WEED_VOLUME_SIZE_LIMIT_MB"
    shift
    exec $WEED server $ARGS $@
    ;;
  's3')
    ARGS="-domainName=$WEED_S3_DOMAIN_NAME -key.file=$WEED_S3_KEY_FILE -cert.file=$WEED_S3_CERT_FILE"
    shift
    exec $WEED s3 $ARGS $@
    ;;
  'cronjob')
    cat <<-EOF >crontab
    $FIX_REPLICATION_CRON_SCHEDULE echo "lock; volume.fix.replication; unlock" | $WEED shell -master=$WEED_MASTER -filer=$WEED_FILER
    $BALANCING_CRON_SCHEDULE echo "lock; volume.balance -collection ALL_COLLECTIONS -force; unlock" | $WEED shell -master=$WEED_MASTER -filer=$WEED_FILER
    EOF
    echo "Running Crontab:"
    cat /crontab
    exec supercronic /crontab
    ;;
  *)
    exec $WEED $@
    ;;
esac
