#include <uwsgi.h>
#include <netdb.h>

#define MAX_BUFFER_SIZE 8192

/*

this is a stats pusher plugin for DogStatsD:

--stats-push dogstatsd:address[,prefix]

example:

--stats-push dogstatsd:127.0.0.1:8125,myinstance
--stats-push dogstatsd:[::1]:8125,myinstance

exports values exposed by the metric subsystem to a Datadog Agent StatsD server

*/

extern struct uwsgi_server uwsgi;

struct dogstatsd_config {
    int no_workers;
    int all_gauges;
    char *extra_tags;
    struct uwsgi_string_list *metrics_whitelist;
} u_dogstatsd_config;

static struct uwsgi_option dogstatsd_options[] = {
  {"dogstatsd-no-workers", no_argument, 0, "disable generation of single-worker metrics", uwsgi_opt_true, &u_dogstatsd_config.no_workers, 0},
  {"dogstatsd-all-gauges", no_argument, 0, "push all metrics to dogstatsd as gauges", uwsgi_opt_true, &u_dogstatsd_config.all_gauges, 0},
  {"dogstatsd-extra-tags", required_argument, 0, "add these extra tags to all metrics (example: foo:bar,qin,baz:qux)", uwsgi_opt_set_str, &u_dogstatsd_config.extra_tags, 0},
  {"dogstatsd-whitelist-metric", required_argument, 0, "use one or more times to send only the whitelisted metrics (do not add prefix)", uwsgi_opt_add_string_list, &u_dogstatsd_config.metrics_whitelist, 0},
  UWSGI_END_OF_OPTIONS
};

// configuration of a dogstatsd node
struct dogstatsd_node {
  int fd;
  struct sockaddr_storage addr;
  socklen_t addr_len;
  char *prefix;
  uint16_t prefix_len;
};

/*
 * Create a non-blocking UDP socket for the given address string.
 *
 * Supports IPv4, IPv6, and hostnames (including AAAA-only DNS records):
 *   "127.0.0.1:8125"       - IPv4 literal
 *   "[::1]:8125"           - IPv6 literal (RFC 3986 bracket notation)
 *   "statsd.svc.local:8125" - hostname resolved via getaddrinfo(AF_UNSPEC)
 *
 * On success, populates addr/addr_len with the resolved address and returns
 * a non-blocking UDP socket file descriptor.
 * On failure, logs the error and returns -1.
 */
static int dogstatsd_create_udp_socket(char *arg, struct sockaddr_storage *addr, socklen_t *addr_len) {
  char *host_start = arg;
  char *port_str = NULL;
  char host_buf[256];
  size_t host_len;

  if (host_start[0] == '[') {
    /* Bracketed IPv6: skip '[', find ']', expect ':' after it */
    host_start++;
    char *bracket = strchr(host_start, ']');
    if (!bracket || bracket[1] != ':') {
      uwsgi_log("invalid dd address %s\n", arg);
      return -1;
    }
    port_str = bracket + 2;
    host_len = bracket - host_start;
  } else {
    /* IPv4 or hostname: split on the first colon */
    char *colon = strchr(host_start, ':');
    if (!colon) {
      uwsgi_log("invalid dd address %s\n", arg);
      return -1;
    }
    port_str = colon + 1;
    host_len = colon - host_start;
  }

  /* Reject empty host or port */
  if (host_len == 0 || host_len >= sizeof(host_buf) || *port_str == '\0') {
    uwsgi_log("invalid dd address %s\n", arg);
    return -1;
  }

  /* Copy host into a local buffer for getaddrinfo (needs null-terminated) */
  memcpy(host_buf, host_start, host_len);
  host_buf[host_len] = '\0';

  /* Resolve address — AF_UNSPEC lets the OS return IPv4 or IPv6 */
  struct addrinfo hints, *res;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_DGRAM;
  hints.ai_flags = AI_NUMERICSERV;

  int gai_ret = getaddrinfo(host_buf, port_str, &hints, &res);
  if (gai_ret != 0) {
    uwsgi_log("invalid dd address %s: %s\n", arg, gai_strerror(gai_ret));
    return -1;
  }

  memcpy(addr, res->ai_addr, res->ai_addrlen);
  *addr_len = res->ai_addrlen;
  int fd = socket(res->ai_family, SOCK_DGRAM, 0);
  freeaddrinfo(res);

  if (fd < 0) {
    uwsgi_error("dogstatsd_create_udp_socket()/socket()");
    return -1;
  }

  uwsgi_socket_nb(fd);
  return fd;
}

static int dogstatsd_generate_tags(char *metric, size_t metric_len, char *datatog_metric_name, char *datadog_tags) {
  char *start = metric;
  size_t metric_offset = 0;

  static char metric_separator[] = ".";
  static char tag_separator[] = ",";
  static char tag_colon = ':';
  static char tag_prefix[] = "|#";

  long string_to_int;
  char *token = NULL;
  char *ctxt = NULL;
  char *key = NULL;
  char *next_character = NULL;

  errno = 0;

  token = strtok_r(start, metric_separator, &ctxt);

  if (!token)
    return -1;

  if (u_dogstatsd_config.extra_tags && strlen(u_dogstatsd_config.extra_tags)) {
    strncat(datadog_tags, tag_prefix, (MAX_BUFFER_SIZE - strlen(datadog_tags) - strlen(tag_prefix) - 1));
    strncat(datadog_tags, u_dogstatsd_config.extra_tags, (MAX_BUFFER_SIZE - strlen(datadog_tags) - strlen(u_dogstatsd_config.extra_tags) - 1));
  }

  while (token != NULL && metric_len >= metric_offset) {

    metric_offset += strlen(token) + 1;
    start = metric + metric_offset;

    // try to convert token into integer
    string_to_int = strtol(token, &next_character, 10);

    // stop processing if string_to_int is out of range
    if ((string_to_int == LONG_MIN || string_to_int == LONG_MAX) && errno == ERANGE)
      return -1;

    // if we've got a number and a tag value:
    if (next_character != token && key) {

      // start with tag_separator if we already have some tags
      //   otherwise put the tag_prefix
      if (strlen(datadog_tags))
       strncat(datadog_tags, tag_separator, (MAX_BUFFER_SIZE - strlen(datadog_tags) - strlen(tag_separator) - 1));
      else
       strncat(datadog_tags, tag_prefix, (MAX_BUFFER_SIZE - strlen(datadog_tags) - strlen(tag_prefix) - 1));

      // append new tag
      strncat(datadog_tags, key, (MAX_BUFFER_SIZE - strlen(datadog_tags) - strlen(key) - 1));
      strncat(datadog_tags, &tag_colon, 1);
      strncat(datadog_tags, token, (MAX_BUFFER_SIZE - strlen(datadog_tags) - strlen(token) - 1));

    } else {

      // store token as a key for the next iteration
      key = token;

      // start with metric_separator if we already have some metrics
      if (strlen(datatog_metric_name))
       strncat(datatog_metric_name, metric_separator, (MAX_BUFFER_SIZE - strlen(datatog_metric_name) - strlen(metric_separator) - 1));

      // add token
      strncat(datatog_metric_name, token, (MAX_BUFFER_SIZE - strlen(datatog_metric_name) - strlen(token) - 1));
    }

    // try to generate tokens before we iterate again
    token = strtok_r(NULL, metric_separator, &ctxt);
  }

  return strlen(datatog_metric_name);
}


static int dogstatsd_send_metric(struct uwsgi_buffer *ub, struct uwsgi_stats_pusher_instance *uspi, char *metric, size_t metric_len, int64_t value, char type[2]) {
  struct dogstatsd_node *sn = (struct dogstatsd_node *) uspi->data;

  char datatog_metric_name[MAX_BUFFER_SIZE];
  char datadog_tags[MAX_BUFFER_SIZE];
  char raw_metric_name[MAX_BUFFER_SIZE];

  int extracted_tags = 0;

  // check if we can handle such a metric length
  if (metric_len >= MAX_BUFFER_SIZE)
    return -1;

  // reset the buffer
  ub->pos = 0;

  // sanitize buffers
  memset(datadog_tags, 0, MAX_BUFFER_SIZE);
  memset(datatog_metric_name, 0, MAX_BUFFER_SIZE);

  // let's copy original metric name before we start
  strncpy(raw_metric_name, metric, metric_len + 1);

  // try to extract tags
  extracted_tags = dogstatsd_generate_tags(raw_metric_name, metric_len, datatog_metric_name, datadog_tags);

  if (extracted_tags < 0)
    return -1;

  if (u_dogstatsd_config.metrics_whitelist && !uwsgi_string_list_has_item(u_dogstatsd_config.metrics_whitelist, datatog_metric_name, strlen(datatog_metric_name))) {
    return 0;
  }

  if (uwsgi_buffer_append(ub, sn->prefix, sn->prefix_len)) return -1;
  if (uwsgi_buffer_append(ub, ".", 1)) return -1;

  // put the datatog_metric_name if we found some tags
  if (extracted_tags) {
    if (uwsgi_buffer_append(ub, datatog_metric_name, strlen(datatog_metric_name))) return -1;
  } else {
    if (uwsgi_buffer_append(ub, metric, strlen(metric))) return -1;
  }

  if (uwsgi_buffer_append(ub, ":", 1)) return -1;
  if (uwsgi_buffer_num64(ub, value)) return -1;
  if (uwsgi_buffer_append(ub, type, 2)) return -1;

  // add tags metadata if there are any
  if (extracted_tags) {
    if (uwsgi_buffer_append(ub, datadog_tags, strlen(datadog_tags))) return -1;
  }

  if (sendto(sn->fd, ub->buf, ub->pos, 0, (struct sockaddr *) &sn->addr, sn->addr_len) < 0) {
    uwsgi_error("dogstatsd_send_metric()/sendto()");
  }

  return 0;
}


static void stats_pusher_dogstatsd(struct uwsgi_stats_pusher_instance *uspi, time_t now, char *json, size_t json_len) {

  if (!uspi->configured) {
    struct dogstatsd_node *sn = uwsgi_calloc(sizeof(struct dogstatsd_node));
    char *comma = strchr(uspi->arg, ',');
    if (comma) {
      sn->prefix = comma+1;
      sn->prefix_len = strlen(sn->prefix);
      *comma = 0;
    }
    else {
      sn->prefix = "uwsgi";
      sn->prefix_len = 5;
    }

    sn->fd = dogstatsd_create_udp_socket(uspi->arg, &sn->addr, &sn->addr_len);
    if (sn->fd < 0) {
      if (comma) *comma = ',';
      free(sn);
      return;
    }
    if (comma) *comma = ',';
    uspi->data = sn;
    uspi->configured = 1;
  }

  // we use the same buffer for all of the packets
  if (uwsgi.metrics_cnt <= 0) {
    uwsgi_log(" *** WARNING: Dogstatsd stats pusher configured but there are no metrics to push ***\n");
    return;
  }

  struct uwsgi_buffer *ub = uwsgi_buffer_new(uwsgi.page_size);
  struct uwsgi_metric *um = uwsgi.metrics;
  while(um) {
    if (u_dogstatsd_config.no_workers && !uwsgi_starts_with(um->name, um->name_len, "worker.", 7)) {
      um = um->next;
      continue;
    }

    uwsgi_rlock(uwsgi.metrics_lock);
    // ignore return value
    if (u_dogstatsd_config.all_gauges || um->type == UWSGI_METRIC_GAUGE) {
      dogstatsd_send_metric(ub, uspi, um->name, um->name_len, *um->value, "|g");
    }
    else {
      dogstatsd_send_metric(ub, uspi, um->name, um->name_len, *um->value, "|c");
    }
    uwsgi_rwunlock(uwsgi.metrics_lock);
    if (um->reset_after_push){
      uwsgi_wlock(uwsgi.metrics_lock);
      *um->value = um->initial_value;
      uwsgi_rwunlock(uwsgi.metrics_lock);
    }
    um = um->next;
  }
  uwsgi_buffer_destroy(ub);
}

static void dogstatsd_init(void) {
  struct uwsgi_stats_pusher *usp = uwsgi_register_stats_pusher("dogstatsd", stats_pusher_dogstatsd);
  // we use a custom format not the JSON one
  usp->raw = 1;
}

struct uwsgi_plugin dogstatsd_plugin = {

    .name = "dogstatsd",
    .options = dogstatsd_options,
    .on_load = dogstatsd_init,
};
