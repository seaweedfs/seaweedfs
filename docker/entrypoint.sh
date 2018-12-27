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
	mkdir -p /data/filerdb
  	exec /usr/bin/weed $@ $ARGS
	;;

  's3')
  	ARGS="-domainName=$S3_DOMAIN_NAME -key.file=$S3_KEY_FILE -cert.file=$S3_CERT_FILE"
  	if [ -n "$FILER_PORT_8888_TCP_ADDR" ] ; then
		ARGS="$ARGS -filer=$FILER_PORT_8888_TCP_ADDR:$FILER_PORT_8888_TCP_PORT"
	fi
  	exec /usr/bin/weed $@ $ARGS
	;;

  *)
  	exec /usr/bin/weed $@
	;;
esac
