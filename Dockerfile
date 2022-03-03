# ubuntu:18.04 on 04/17/2019
FROM ubuntu@sha256:017eef0b616011647b269b5c65826e2e2ebddbe5d1f8c1e56b3599fb14fabec8

MAINTAINER Systems Infrastructure <systems-team@nextdoor.com>

# Versions don't matter here.. any apache2/curl version will due
RUN DEBIAN_FRONTEND=noninteractive apt-get update; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y apache2 curl

# Enable SSL in Apache - this also generates some snakeoil fake certs
RUN a2enmod ssl
RUN cp /etc/apache2/sites-available/default-ssl.conf /etc/apache2/sites-enabled

ADD www /var/www/html
ADD resources /app

EXPOSE 443
ENTRYPOINT ["/app/entrypoint.sh"]
