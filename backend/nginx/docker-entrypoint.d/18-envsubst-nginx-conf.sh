#!/bin/sh
set -e

if [ -f /etc/nginx/nginx.conf.template ]; then
    echo "18-envsubst-nginx-conf.sh: Substituting environment variables into /etc/nginx/nginx.conf..."
    envsubst '${NGINX_WORKER_PROCESSES} ${NGINX_WORKER_CONNECTIONS} ${NGINX_WORKER_RLIMIT_NOFILE} ${NGINX_KEEPALIVE_TIMEOUT} ${NGINX_CLIENT_MAX_BODY_SIZE} ${NGINX_GZIP_ENABLED} ${NGINX_GZIP_COMP_LEVEL}' < /etc/nginx/nginx.conf.template > /etc/nginx/nginx.conf
fi
