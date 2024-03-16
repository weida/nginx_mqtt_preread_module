# Nginx Stream Pre-read MQTT Module

Nginx module for reading  "client" from a stream connection using MQTT protocol.

## Build

![Build Status](https://github.com/kaltura/nginx-stream-preread-str-module/actions/workflows/ci.yml/badge.svg)


    auto/configure --add-module=/path/to/nginx_mqtt_preread_module --with-stream


## Configuration

### Sample configuration

```
stream {

      log_format basic '$remote_addr [$time_local] '
                 'clientid: [$mqtt_preread_clientid]  username: [$mqtt_preread_username] '
                 'password: [$mqtt_preread_password] '
                 'protocol: [$mqtt_preread_protocol_name]  version: [$mqtt_preread_protocol_version] ';

      access_log /root/install/nginx/logs/nginx-access.log basic;
      mqtt_preread on;

      upstream backend {
          hash $mqtt_preread_clientid;

          server 10.0.0.7:1883; # upstream mqtt broker 1
          server 10.0.0.8:1883; # upstream mqtt broker 2
          server 10.0.0.9:1883; # upstream mqtt broker 3
      }

      server {
          listen 2883;
          proxy_pass backend;
          proxy_connect_timeout 1s;
      }
}

```

### Configuration directives

#### mqtt_preread
* **syntax**: `	mqtt_preread on | off;`
* **default**: `mqtt_preread off;`
* **context**: `stream, server`

Enables extracting information from the MQTT CONNECT message at the preread phase.

## Embedded Variables

* `$mqtt_preread_clientid` - the clientid value from the CONNECT message
* `$mqtt_preread_username` - the username value from the CONNECT message
* `$mqtt_preread_password` - the password value from the CONNECT message
* `$mqtt_preread_protocol_name` - the protocol name value from the CONNECT message
* `$mqtt_preread_protocol_version` - the protocol version value from the CONNECT message



