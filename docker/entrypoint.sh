#!/bin/sh

case "$1" in

  'master')
  	ARGS="-ip `hostname -i` -mdir /data"
	# Is this instance linked with an other master? (Docker commandline "--link master1:master")
	if [ -n "$MASTER_PORT_9333_TCP_ADDR" ] ; then
		ARGS="$ARGS -peers=$MASTER_PORT_9333_TCP_ADDR:$MASTER_PORT_9333_TCP_PORT"
	fi
  	exec /usr/bin/weed $@ $ARGS
	;;

  'volume')
  	ARGS="-ip `hostname -i` -dir /data"
	# Is this instance linked with a master? (Docker commandline "--link master1:master")
  	if [ -n "$MASTER_PORT_9333_TCP_ADDR" ] ; then
		ARGS="$ARGS -mserver=$MASTER_PORT_9333_TCP_ADDR:$MASTER_PORT_9333_TCP_PORT"
	fi
  	exec /usr/bin/weed $@ $ARGS
	;;

  'server')
  	ARGS="-ip `hostname -i` -dir /data"
  	if [ -n "$MASTER_PORT_9333_TCP_ADDR" ] ; then
		ARGS="$ARGS -master.peers=$MASTER_PORT_9333_TCP_ADDR:$MASTER_PORT_9333_TCP_PORT"
	fi
  	exec /usr/bin/weed $@ $ARGS
  	;;

  'filer')
  	ARGS="-ip `hostname -i` "
  	if [ -n "$MASTER_PORT_9333_TCP_ADDR" ] ; then
		ARGS="$ARGS -master=$MASTER_PORT_9333_TCP_ADDR:$MASTER_PORT_9333_TCP_PORT"
	fi
  	exec /usr/bin/weed $@ $ARGS
	;;

  's3')
  	ARGS="-domainName=$S3_DOMAIN_NAME -key.file=$S3_KEY_FILE -cert.file=$S3_CERT_FILE"
  	if [ -n "$FILER_PORT_8888_TCP_ADDR" ] ; then
		ARGS="$ARGS -filer=$FILER_PORT_8888_TCP_ADDR:$FILER_PORT_8888_TCP_PORT"
	fi
  	exec /usr/bin/weed $@ $ARGS
	;;

  'cronjob')
	MASTER=${WEED_MASTER-localhost:9333}
	FIX_REPLICATION_CRON_SCHEDULE=${CRON_SCHEDULE-*/7 * * * * *}
	echo "$FIX_REPLICATION_CRON_SCHEDULE" 'echo "volume.fix.replication" | weed shell -master='$MASTER > /crontab
	BALANCING_CRON_SCHEDULE=${CRON_SCHEDULE-25 * * * * *}
	echo "$BALANCING_CRON_SCHEDULE" 'echo "volume.balance -c ALL -f" | weed shell -master='$MASTER >> /crontab
	echo "Running Crontab:"
	cat /crontab
	exec supercronic /crontab
	;;
  *)
  	exec /usr/bin/weed $@
	;;
esac
