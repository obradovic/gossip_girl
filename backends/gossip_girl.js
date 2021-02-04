var util = require("util"),
  dgram = require("dgram");

function GossipGirl(startupTime, config, emitter) {
  var self = this;
  self.config = config.gossip_girl || [];
  self.statsd_config = config;
  self.ignorable = ["statsd.packets_received", "statsd.bad_lines_seen", "statsd.packet_process_time"];
  self.sock = dgram.createSocket("udp4");
  self.sock.on("error", function (err) {
    console.log(err);
  });

  emitter.on("flush", function (time_stamp, metrics) {
    self.process(time_stamp, metrics);
  });
}

GossipGirl.prototype.gossip = function (packet, host, port) {
  var self = this;
  self.sock.send(packet, 0, packet.length, port, host);
};

GossipGirl.prototype.format = function (key, value, suffix) {
  return new Buffer("'" + key + "':" + value + "|" + suffix);
};

GossipGirl.prototype.process = async function (time_stamp, metrics) {
  var self = this;
  var hosts = self.config;
  var chunk = self.statsd_config.chunk || 30;
  var chunkThreshold = self.statsd_config.chunkThreshold || 100;
  var delaySend = self.statsd_config.delaySend || 0;
  var stats_map = {
    counters: {data: metrics.counters, suffix: "c", name: "counter"},
    gauges: {data: metrics.gauges, suffix: "g", name: "gauge"},
    timers: {data: metrics.timers, suffix: "ms", name: "timer"}
  };
  
  const items = [];

  hosts.forEach(
    function (host) {
      Object.keys(stats_map).forEach(
        function (type) {
          var stats = stats_map[type];
          Object.keys(stats.data).forEach(
            function (key) {
              if (self.ignorable.indexOf(key) >= 0) {
                return;
              }
              if (type === "counters" && !stats.data[key]) {
                delete stats.data[key];
                return;
              }
              //timers is array
              var values = [].concat(stats.data[key]);
              // for timers split array into 100 chunks and calculate mean value for each chunk (perfmance reasons)
              if (type === "timers" && values.length > chunkThreshold) {
                // util.log(`Gossip timing ${key} exceeded chunk threshold ${values.length} > ${chunkThreshold}, truncate`);
                var chunkSize = Math.ceil(values.length / chunk) ;
                var rest = values;
                var result = [Math.max(...values), Math.min(...values)];
                do {
                  var part = rest.splice(-chunkSize);
                  result.push(mean(part));
                  if (part.length > 10) {
                    result.push(Math.max(...part));
                    result.push(Math.min(...part));
                  }
                } while(rest.length > 0);

                values = result;
              }

              values.forEach(
                function (value) {
                  items.push({
                    name: stats.name,
                    packet: self.format(key, value, stats.suffix),
                    host: host.host,
                    port: host.port
                  });
                }
              );
            }
          );
        }
      );
    }
  );

  for (let item of items) {
    if (self.statsd_config.dumpMessages) {
      util.log("Gossiping about " + item.name + ": " + item.packet);
    }

    self.gossip(item.packet, item.host, item.port);
    if (delaySend > 0) {
      await wait(delaySend);
    }
  }
};

function mean(numbers) {
  return numbers.reduce((acc, val) => acc + val, 0) / numbers.length;
}

function wait(ms) {
  return new Promise(
    (resolve) => setTimeout(() => resolve(), ms)
  );
}

exports.init = function (startupTime, config, events) {
  var instance = new GossipGirl(startupTime, config, events);
  return true;
};
