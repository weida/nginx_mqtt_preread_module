#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_stream.h>

typedef struct {
    ngx_flag_t      enabled;
} ngx_stream_mqtt_preread_srv_conf_t;


typedef struct {
    u_char          buf[4];
    u_char          flags;
    ngx_str_t       clientid;
    ngx_str_t       username;
    ngx_str_t       password;
    ngx_str_t       protocol_name;
    ngx_str_t       protocol_version;
    ngx_str_t       property;
    ngx_str_t       will_properties;
    ngx_str_t       will_topic;
    ngx_str_t       will_message;
    ngx_log_t      *log;
    ngx_pool_t     *pool;
} ngx_stream_mqtt_preread_ctx_t;


static ngx_int_t ngx_stream_mqtt_preread_parse_record(
    ngx_stream_mqtt_preread_ctx_t *ctx, u_char *pos, u_char *last);
static ngx_int_t ngx_stream_mqtt_preread_clientid_variable(
    ngx_stream_session_t *s, ngx_stream_variable_value_t *v, uintptr_t data);
static ngx_int_t ngx_stream_mqtt_preread_username_variable(
    ngx_stream_session_t *s, ngx_stream_variable_value_t *v, uintptr_t data);
static ngx_int_t ngx_stream_mqtt_preread_password_variable(
    ngx_stream_session_t *s, ngx_stream_variable_value_t *v, uintptr_t data);
static ngx_int_t ngx_stream_mqtt_preread_protocol_name_variable(
    ngx_stream_session_t *s, ngx_stream_variable_value_t *v, uintptr_t data);
static ngx_int_t ngx_stream_mqtt_preread_protocol_version_variable(
    ngx_stream_session_t *s, ngx_stream_variable_value_t *v, uintptr_t data);
static ngx_int_t ngx_stream_mqtt_preread_add_variables(ngx_conf_t *cf);
static void *ngx_stream_mqtt_preread_create_srv_conf(ngx_conf_t *cf);
static char *ngx_stream_mqtt_preread_merge_srv_conf(ngx_conf_t *cf,
    void *parent, void *child);
static ngx_int_t ngx_stream_mqtt_preread_init(ngx_conf_t *cf);
static ngx_uint_t ngx_stream_mqtt_decode_packet(u_char *p, size_t *len);


static ngx_command_t  ngx_stream_mqtt_preread_commands[] = {

    { ngx_string("mqtt_preread"),
      NGX_STREAM_MAIN_CONF|NGX_STREAM_SRV_CONF|NGX_CONF_TAKE1,
      ngx_conf_set_flag_slot,
      NGX_STREAM_SRV_CONF_OFFSET,
      offsetof(ngx_stream_mqtt_preread_srv_conf_t, enabled),
      NULL },

      ngx_null_command
};


static ngx_stream_module_t  ngx_stream_mqtt_preread_module_ctx = {
    ngx_stream_mqtt_preread_add_variables,    /* preconfiguration */
    ngx_stream_mqtt_preread_init,             /* postconfiguration */

    NULL,                                     /* create main configuration */
    NULL,                                     /* init main configuration */

    ngx_stream_mqtt_preread_create_srv_conf,  /* create server configuration */
    ngx_stream_mqtt_preread_merge_srv_conf    /* merge server configuration */
};


ngx_module_t  ngx_stream_mqtt_preread_module = {
    NGX_MODULE_V1,
    &ngx_stream_mqtt_preread_module_ctx,      /* module context */
    ngx_stream_mqtt_preread_commands,         /* module directives */
    NGX_STREAM_MODULE,                        /* module type */
    NULL,                                     /* init master */
    NULL,                                     /* init module */
    NULL,                                     /* init process */
    NULL,                                     /* init thread */
    NULL,                                     /* exit thread */
    NULL,                                     /* exit process */
    NULL,                                     /* exit master */
    NGX_MODULE_V1_PADDING
};


static ngx_stream_variable_t  ngx_stream_mqtt_preread_vars[] = {

    { ngx_string("mqtt_preread_clientid"), NULL,
      ngx_stream_mqtt_preread_clientid_variable, 0, 0, 0 },

    { ngx_string("mqtt_preread_username"), NULL,
      ngx_stream_mqtt_preread_username_variable, 0, 0, 0 },

    { ngx_string("mqtt_preread_password"), NULL,
      ngx_stream_mqtt_preread_password_variable, 0, 0, 0 },

    { ngx_string("mqtt_preread_protocol_name"), NULL,
      ngx_stream_mqtt_preread_protocol_name_variable, 0, 0, 0 },

    { ngx_string("mqtt_preread_protocol_version"), NULL,
      ngx_stream_mqtt_preread_protocol_version_variable, 0, 0, 0 },

      ngx_stream_null_variable
};


static ngx_int_t
ngx_stream_mqtt_preread_handler(ngx_stream_session_t *s)
{
    u_char                              *last, *p;
    size_t                               len, size;
    ngx_int_t                            rc;
    ngx_connection_t                    *c;
    ngx_stream_mqtt_preread_ctx_t       *ctx;
    ngx_stream_mqtt_preread_srv_conf_t  *mscf;

    c = s->connection;

    ngx_log_debug0(NGX_LOG_DEBUG_STREAM, c->log, 0, "mqtt preread handler");

    mscf = ngx_stream_get_module_srv_conf(s, ngx_stream_mqtt_preread_module);

    if (!mscf->enabled) {
        return NGX_DECLINED;
    }

    if (c->type != SOCK_STREAM) {
        return NGX_DECLINED;
    }

    if (c->buffer == NULL) {
        return NGX_AGAIN;
    }

    ctx = ngx_stream_get_module_ctx(s, ngx_stream_mqtt_preread_module);
    if (ctx == NULL) {
        ctx = ngx_pcalloc(c->pool, sizeof(ngx_stream_mqtt_preread_ctx_t));
        if (ctx == NULL) {
            return NGX_ERROR;
        }

        ngx_stream_set_ctx(s, ctx, ngx_stream_mqtt_preread_module);

        ctx->pool = c->pool;
        ctx->log = c->log;
    }

    p = c->buffer->pos;
    last = c->buffer->last;

    if (last - p >= 5) {

        if (p[0] != 0x10) {
            ngx_log_debug0(NGX_LOG_DEBUG_STREAM, ctx->log, 0,
                           "mqtt preread: not a CONNECT packet");
            ngx_stream_set_ctx(s, NULL, ngx_stream_mqtt_preread_module);
            return NGX_DECLINED;
        }
        p = p + 1;
        size = ngx_stream_mqtt_decode_packet(p, &len);

        if ( size == 0 || len == 0 ) {
            ngx_log_debug0(NGX_LOG_DEBUG_STREAM, ctx->log, 0,
                           "mqtt preread: Remaining Length format error");
            ngx_stream_set_ctx(s, NULL, ngx_stream_mqtt_preread_module);
            return NGX_DECLINED;
        }

        /* read the whole record before parsing */
        if ((size_t) (last - p) < len + size) {
            ngx_log_debug0(NGX_LOG_DEBUG_STREAM, ctx->log, 0,
                           "mqtt preread: read the whole record ");
            return NGX_AGAIN;
        }

        p = p + size;

        ngx_log_debug2(NGX_LOG_DEBUG_STREAM, ctx->log, 0,
                       "mqtt preread: CONN Header %d Paylen %d", size, len);

        rc = ngx_stream_mqtt_preread_parse_record(ctx, p, p + len);

        if (rc == NGX_DECLINED) {
            ngx_stream_set_ctx(s, NULL, ngx_stream_mqtt_preread_module);
            ngx_log_debug0(NGX_LOG_DEBUG_STREAM, ctx->log, 0,
                           "mqtt preread: Remaining Length format error");
            return NGX_DECLINED;
        }
        ngx_log_debug1(NGX_LOG_DEBUG_STREAM, ctx->log, 0,
                      "clientid %V", &ctx->clientid );
        ngx_log_debug1(NGX_LOG_DEBUG_STREAM, ctx->log, 0,
                      "username %V", &ctx->username );
        ngx_log_debug1(NGX_LOG_DEBUG_STREAM, ctx->log, 0,
                      "password %V", &ctx->password);
        ngx_log_debug1(NGX_LOG_DEBUG_STREAM, ctx->log, 0,
                      "protocol %V", &ctx->protocol_name);
        ngx_log_debug1(NGX_LOG_DEBUG_STREAM, ctx->log, 0,
                      "protocol version %V", &ctx->protocol_version);

        return rc;

    }

    return NGX_AGAIN;
}


static ngx_int_t
ngx_stream_mqtt_preread_parse_record(ngx_stream_mqtt_preread_ctx_t *ctx,
    u_char *pos, u_char *last)
{
 
    size_t       left, n, size, len;
    u_char      *dst, *p;


    enum {
        sw_start = 0,
        sw_protocol_name_len,   /* protocol name length*/
        sw_protocol_name,       /* protocol name */
        sw_protocol_version,    /* protocol version */
        sw_connect_flags,       /* username, will and clean session flags*/
        sw_keepalive_timer,     /* keepalive timer*/
        sw_property_len,        /* property length*/
        sw_property,            /* property */
        sw_ext,                 /* extension*/
        sw_client_id_len,       /* client identifier length */
        sw_client_id,           /* client identifier */
        sw_will_properties_len, /* client identifier */
        sw_will_properties,     /* client identifier */
        sw_will_topic_len,      /* will topic length*/
        sw_will_topic,          /* will topic */
        sw_will_message_len,    /* will message length*/
        sw_will_message,        /* will message */
        sw_user_name_len,       /* user name length*/
        sw_user_name,           /* user name */
        sw_password_len,        /* password length*/
        sw_password,            /* password */
    } state;

    size = 0;
    dst = NULL;
    state = sw_start;
    left = last - pos;

    ngx_log_debug2(NGX_LOG_DEBUG_STREAM, ctx->log, 0,
                   "mqtt preread: state %ui left %z", state, left);

    p = ctx->buf;

    for ( ;; ) {
        n = ngx_min((size_t) (last - pos), size);

        if (dst) {
            dst = ngx_cpymem(dst, pos, n);
            ngx_log_debug2(NGX_LOG_DEBUG_STREAM, ctx->log, 0,
                           "mqtt preread: state %ui n %d", state, n);
        }
        pos += n; 
        size -= n; 
        left -= n; 

        if (size != 0) {
            break;
        }

        switch (state) {

        case sw_start:
            state = sw_protocol_name_len;
            dst = p;
            size = 2;
            break;

        case sw_protocol_name_len:
	    size = (p[0]<< 8) + p[1];
            ctx->protocol_name.len = size;
            ctx->protocol_name.data = ngx_pnalloc(ctx->pool, size);
            if ( ctx->protocol_name.data  == NULL) {
                return NGX_ERROR;
            }
            state = sw_protocol_name;
            dst = ctx->protocol_name.data;
            break;

        case sw_protocol_name:
            ngx_log_debug1(NGX_LOG_DEBUG_STREAM, ctx->log, 0,
                     "mqtt preread: Protocol Name\"%V\"", &ctx->protocol_name);

            state = sw_protocol_version;
            dst = p;
            size = 1;
            break;

        case sw_protocol_version:
            ctx->protocol_version.data = ngx_pnalloc(ctx->pool, 1);
            if ( ctx->protocol_version.data  == NULL) {
                return NGX_ERROR;
            }
            ctx->protocol_version.data[0] = p[0] + 0x30;
            ctx->protocol_version.len = 1;
            ngx_log_debug1(NGX_LOG_DEBUG_STREAM, ctx->log, 0,
               "mqtt preread: Protocol version\"%ui\"", ctx->protocol_version);

            state = sw_connect_flags;
            dst = p;
            size = 1;
            break;

        case sw_connect_flags:
            ctx->flags = p[0];
            ngx_log_debug1(NGX_LOG_DEBUG_STREAM, ctx->log, 0,
                           "mqtt preread: Connect flags\"%Xd\"", ctx->flags);
            state = sw_keepalive_timer;
            dst = p;
            size = 2;
            break;

        case sw_keepalive_timer:
            if (ctx->protocol_version.data[0] == '5') {
                state = sw_property_len;
                size = ngx_stream_mqtt_decode_packet(pos, &len);
                ngx_log_debug2(NGX_LOG_DEBUG_STREAM, ctx->log, 0,
                             "mqtt preread: len %ud size %ud", len, size);
                ctx->property.len = len;
                dst = NULL;
            } else {
                state = sw_client_id_len;
                size = 2;
                dst = p;
            }
            break;

        case sw_property_len:
            size = ctx->property.len;
            ctx->property.data = ngx_pnalloc(ctx->pool, size);
            if ( ctx->property.data  == NULL) {
                return NGX_ERROR;
            }
            ngx_log_debug1(NGX_LOG_DEBUG_STREAM, ctx->log, 0,
                           "mqtt preread: Property len\"%ui\"", size);
            state = sw_property;
            dst = ctx->property.data;
            break;

        case sw_property:
            ngx_log_debug1(NGX_LOG_DEBUG_STREAM, ctx->log, 0,
                           "mqtt preread: Properties \"%V\"", &ctx->property);
            state = sw_client_id_len;
            size = 2;
            dst = p;
            break;

        case sw_ext:
            if (left == 0) {
                return NGX_OK;
            }

            if (ctx->protocol_version.data[0] == '5' ) {
                if ((ctx->flags & 0x04) && ctx->will_properties.data == NULL) {
                    state = sw_will_properties_len;
                    size = ngx_stream_mqtt_decode_packet(pos, &len);
                    ngx_log_debug2(NGX_LOG_DEBUG_STREAM, ctx->log, 0,
                                 "mqtt preread: len %ud size %ud", len, size);
                    ctx->will_properties.len = len;
                    dst = NULL;
                    break;
                }
            } else {
                if ((ctx->flags & 0x04) && ctx->will_topic.data == NULL) {
                    state = sw_will_topic_len;
                    size = 2;
                    dst = p;
                    break;
                }
            }

            if ((ctx->flags & 0x80) && ctx->username.data == NULL) {
                state = sw_user_name_len;
                dst = p;
                size = 2;
                break;
            }

            if ((ctx->flags & 0x40) && ctx->password.data == NULL) {
                state = sw_password_len;
                dst = p;
                size = 2;
                break;
            }

            break;

        case sw_client_id_len:       
            size = (p[0]<< 8) + p[1];
            ngx_log_debug1(NGX_LOG_DEBUG_STREAM, ctx->log, 0,
                           "mqtt preread: Client id Len\"%ui\"", size);
            ctx->clientid.data = ngx_pnalloc(ctx->pool, size);
            if ( ctx->clientid.data  == NULL) {
                return NGX_ERROR;
            }
            state = sw_client_id;
            dst = ctx->clientid.data;
            break;

        case sw_client_id:
            ctx->clientid.len = (p[0]<< 8) + p[1];
            ngx_log_debug1(NGX_LOG_DEBUG_STREAM, ctx->log, 0,
                           "mqtt preread: Client id \"%V\"", &ctx->clientid);
            state = sw_ext;
            dst = NULL;
            size = 0;
            break;

        case sw_will_properties_len:
            size = ctx->will_properties.len;
            ctx->will_properties.data = ngx_pnalloc(ctx->pool, size);
            if ( ctx->will_properties.data  == NULL) {
                return NGX_ERROR;
            }
            state = sw_will_properties;
            dst = ctx->will_properties.data;
            break;

        case sw_will_properties:
            ctx->will_properties.len = (p[0]<< 8) + p[1];
            ngx_log_debug1(NGX_LOG_DEBUG_STREAM, ctx->log, 0,
                           "mqtt preread: will Properties \"%V\"", &ctx->will_properties);
            state = sw_will_topic_len;
            dst = p;
            size = 2;
            break;

        case sw_will_topic_len:
            size = (p[0]<< 8) + p[1];
            ctx->will_topic.len = size;
            ctx->will_topic.data = ngx_pnalloc(ctx->pool, size);
            if ( ctx->will_topic.data  == NULL) {
                return NGX_ERROR;
            }
            state = sw_will_topic;
            dst = ctx->will_topic.data;
            break;

        case sw_will_topic:
            ngx_log_debug1(NGX_LOG_DEBUG_STREAM, ctx->log, 0,
                           "mqtt preread: Will topic \"%V\"", &ctx->will_topic);
            state = sw_will_message_len;
            dst = p;
            size = 2;
            break;

        case sw_will_message_len:
            size = (p[0]<< 8) + p[1];
            ctx->will_topic.len = size;
            ctx->will_topic.data = ngx_pnalloc(ctx->pool, size);
            if ( ctx->will_topic.data  == NULL) {
                return NGX_ERROR;
            }
            state = sw_will_message;
            dst = ctx->will_topic.data;
            break;

        case sw_will_message:
            ngx_log_debug1(NGX_LOG_DEBUG_STREAM, ctx->log, 0,
                           "mqtt preread: Will Messsge \"%V\"", &ctx->will_message);
            state = sw_user_name_len;
            dst = p;
            size = 2;
            break;

        case sw_user_name_len:
            size = (p[0]<< 8) + p[1];
            ctx->username.data = ngx_pnalloc(ctx->pool, size);
            if ( ctx->username.data  == NULL) {
                return NGX_ERROR;
            }
            state = sw_user_name;
            dst = ctx->username.data;
            break;
        case sw_user_name:
            ctx->username.len = (p[0]<< 8) + p[1];
            ngx_log_debug1(NGX_LOG_DEBUG_STREAM, ctx->log, 0,
                           "mqtt preread: Username \"%V\"", &ctx->username);
            state = sw_ext;
            dst = NULL;
            size = 0;
            break;
    
        case sw_password_len:
            size = (p[0]<< 8) + p[1];
            ctx->password.data = ngx_pnalloc(ctx->pool, size);
            if ( ctx->password.data  == NULL) {
                return NGX_ERROR;
            }
            state = sw_password;
            dst = ctx->password.data;
            break;
        case sw_password:
            ctx->password.len = (p[0]<< 8) + p[1];
            ngx_log_debug1(NGX_LOG_DEBUG_STREAM, ctx->log, 0,
                           "mqtt preread: password \"%V\"", &ctx->password);
            if (left == 0) {
                return NGX_OK;
            }
            break;

        }
        if (left < size) {
            ngx_log_debug0(NGX_LOG_DEBUG_STREAM, ctx->log, 0,
                           "mqtt preread: failed to parse handshake");
            return NGX_DECLINED;
        }

    }
    return NGX_DECLINED;
}

static ngx_uint_t
ngx_stream_mqtt_decode_packet(u_char *p, size_t *len) 
{
    ngx_int_t   multiplier;
    ngx_uint_t  i;

    *len = 0;
    if (p == NULL) {
        return 0;
    }

    multiplier = 1;
    for (i = 0; i < 4; i++ ) {
        *len += (p[i] & 127) * multiplier;
        multiplier *= 128;
        if ( (p[i] & 0x80) == 0 ) {
            break;
        }
    }
    return i+1;
}


static ngx_int_t
ngx_stream_mqtt_preread_clientid_variable(ngx_stream_session_t *s,
    ngx_stream_variable_value_t *v, uintptr_t data)
{
    ngx_stream_mqtt_preread_ctx_t  *ctx;

    ctx = ngx_stream_get_module_ctx(s, ngx_stream_mqtt_preread_module);

    if (ctx == NULL) {
        v->not_found = 1;
        return NGX_OK;
    }

    v->valid = 1;
    v->no_cacheable = 0;
    v->not_found = 0;
    v->len = ctx->clientid.len;
    v->data = ctx->clientid.data;

    return NGX_OK;
}


static ngx_int_t
ngx_stream_mqtt_preread_username_variable(ngx_stream_session_t *s,
    ngx_stream_variable_value_t *v, uintptr_t data)
{
    ngx_stream_mqtt_preread_ctx_t  *ctx;

    ctx = ngx_stream_get_module_ctx(s, ngx_stream_mqtt_preread_module);

    if (ctx == NULL) {
        v->not_found = 1;
        return NGX_OK;
    }

    v->valid = 1;
    v->no_cacheable = 0;
    v->not_found = 0;
    v->len = ctx->username.len;
    v->data = ctx->username.data;

    return NGX_OK;
}

static ngx_int_t
ngx_stream_mqtt_preread_password_variable(ngx_stream_session_t *s,
    ngx_stream_variable_value_t *v, uintptr_t data)
{
    ngx_stream_mqtt_preread_ctx_t  *ctx;

    ctx = ngx_stream_get_module_ctx(s, ngx_stream_mqtt_preread_module);

    if (ctx == NULL) {
        v->not_found = 1;
        return NGX_OK;
    }

    v->valid = 1;
    v->no_cacheable = 0;
    v->not_found = 0;
    v->len = ctx->password.len;
    v->data = ctx->password.data;

    return NGX_OK;
}

static ngx_int_t
ngx_stream_mqtt_preread_protocol_name_variable(ngx_stream_session_t *s,
    ngx_stream_variable_value_t *v, uintptr_t data)
{
    ngx_stream_mqtt_preread_ctx_t  *ctx;

    ctx = ngx_stream_get_module_ctx(s, ngx_stream_mqtt_preread_module);

    if (ctx == NULL) {
        v->not_found = 1;
        return NGX_OK;
    }

    v->valid = 1;
    v->no_cacheable = 0;
    v->not_found = 0;
    v->len = ctx->protocol_name.len;
    v->data = ctx->protocol_name.data;

    return NGX_OK;
}

static ngx_int_t
ngx_stream_mqtt_preread_protocol_version_variable(ngx_stream_session_t *s,
    ngx_stream_variable_value_t *v, uintptr_t data)
{
    ngx_stream_mqtt_preread_ctx_t  *ctx;

    ctx = ngx_stream_get_module_ctx(s, ngx_stream_mqtt_preread_module);

    if (ctx == NULL) {
        v->not_found = 1;
        return NGX_OK;
    }

    v->valid = 1;
    v->no_cacheable = 0;
    v->not_found = 0;
    v->len = ctx->protocol_version.len;
    v->data = ctx->protocol_version.data;

    return NGX_OK;
}




static ngx_int_t
ngx_stream_mqtt_preread_add_variables(ngx_conf_t *cf)
{
    ngx_stream_variable_t  *var, *v;

    for (v = ngx_stream_mqtt_preread_vars; v->name.len; v++) {
        var = ngx_stream_add_variable(cf, &v->name, v->flags);
        if (var == NULL) {
            return NGX_ERROR;
        }

        var->get_handler = v->get_handler;
        var->data = v->data;
    }

    return NGX_OK;
}


static void *
ngx_stream_mqtt_preread_create_srv_conf(ngx_conf_t *cf)
{
    ngx_stream_mqtt_preread_srv_conf_t  *conf;

    conf = ngx_pcalloc(cf->pool, sizeof(ngx_stream_mqtt_preread_srv_conf_t));
    if (conf == NULL) {
        return NULL;
    }

    conf->enabled = NGX_CONF_UNSET;

    return conf;
}


static char *
ngx_stream_mqtt_preread_merge_srv_conf(ngx_conf_t *cf, void *parent,
    void *child)
{
    ngx_stream_mqtt_preread_srv_conf_t  *prev = parent;
    ngx_stream_mqtt_preread_srv_conf_t  *conf = child;

    ngx_conf_merge_value(conf->enabled, prev->enabled, 0);

    return NGX_CONF_OK;
}


static ngx_int_t
ngx_stream_mqtt_preread_init(ngx_conf_t *cf)
{
    ngx_stream_handler_pt        *h;
    ngx_stream_core_main_conf_t  *cmcf;

    cmcf = ngx_stream_conf_get_module_main_conf(cf, ngx_stream_core_module);

    h = ngx_array_push(&cmcf->phases[NGX_STREAM_PREREAD_PHASE].handlers);
    if (h == NULL) {
        return NGX_ERROR;
    }

    *h = ngx_stream_mqtt_preread_handler;

    return NGX_OK;
}
