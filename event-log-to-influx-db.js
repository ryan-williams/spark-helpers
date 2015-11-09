
var filename = process.argv[2];

var fs = require('fs');
var oboe = require('oboe');

var is = fs.createReadStream(filename);

var appId = null;
var appName = null;

var events = [];

var stages = {};
var lastTime = null;

function push(node, key, start, timeKey, tagsObj, fieldsObj) {
  var event = {
    key: key,
    tags: tagsObj || {},
    fields: fieldsObj || {}
  };

  event.tags.start = !!start;

  if (timeKey instanceof Array) {
    lastTime = timeKey.reduce(function(o, segment) {
      return (segment in o) ? o[segment] : null;
    }, node);
  } else {
    var curTime = node[timeKey || 'Timestamp'];
    if (curTime) {
      lastTime = curTime;
    } else if (timeKey != null) {
      console.error("Missing time %s:", timeKey, node);
    }
  }
  if (lastTime) {
    event.timestamp = lastTime;
  } else {
    if (timeKey) {
      console.error("No timestamp '%s' found for:", timeKey, node);
    }
  }

  events.push(event);
  return event;
}

function flattenObj(o, kvSep, fieldSep) {
  var r = [];
  for (var k in o) {
    var v = o[k];
    if (typeof v === 'string') {
      v = v.replace(/ /g, '\\ ');
    }
    var a = [ k, v ];
    if (kvSep) {
      a = a.join(kvSep);
    }
    r.push(a);
  }
  if (fieldSep) {
    r = r.join(fieldSep);
  }
  return r;
}

console.log("Reading from:", filename);
oboe(is).node('events[*]', function(node) {
  if (node.Event == "SparkListenerStageSubmitted") {

    var stageInfo = node['Stage Info'];
    var stageId = stageInfo['Stage ID'];
    var attemptId = stageInfo['Stage Attempt ID'];

    var event = push(node, 'stage', true, null, { id: stageId + '.' + attemptId });
    if (lastTime) {
      event.timestamp = lastTime;
    } else {
      console.error("No time available for stage:", node);
    }

    if (!(stageId in stages)) {
      stages[stageId] = {};
    }
    stages[stageId][attemptId] = event;

  } else if (node.Event == "SparkListenerStageCompleted") {
    var stageInfo = node['Stage Info'];
    var stageId = stageInfo['Stage ID'];
    var attemptId = stageInfo['Stage Attempt ID'];

    push(node, 'stage', false, ['Stage Info', 'Completion Time'], { id: stageId + '.' + attemptId });

    var submissionTime = stageInfo['Submission Time'];
    if (submissionTime) {
      stages[stageInfo['Stage ID']][stageInfo['Stage Attempt ID']].timestamp = submissionTime;
    } else {
      console.error("Missing submission time on StageCompletion event:", node);
    }

  } else if (node.Event == "SparkListenerJobStart") {
    push(node, 'job', true, 'Submission Time');
  } else if (node.Event == "SparkListenerJobEnd") {
    push(node, 'job', false, 'Completion Time', { success: node['Job Result']['Result'] == 'JobSucceeded' });
  } else if (node.Event == "SparkListenerTaskStart") {
    push(node, 'task', true, ['Task Info', 'Launch Time']);
  } else if (node.Event == "SparkListenerTaskEnd") {
    var reason = node["Task End Reason"]["Reason"];
    var success = !node['Task Info']['Failed'];
    push(node, 'task', false, ['Task Info', 'Finish Time'], success ? { success: true } : { success: false, reason: reason });
  } else if (node.Event == "SparkListenerApplicationStart") {
    appId = node["App ID"];
    appName = node["App Name"];
    push(node, 'app', true, 'Timestamp', { name: appName });
  } else if (node.Event == "SparkListenerApplicationEnd") {
    push(node, 'app');
  } else if (node.Event == "SparkListenerExecutorAdded") {
    push(node, 'executor', true);
  } else if (node.Event == "SparkListenerExecutorRemoved") {
    push(node, 'executor', false, 'Timestamp', { reason: node['Removed Reason'] });
  } else if (node.Event == "SparkListenerBlockManagerAdded") {
    push(node, 'block_manager', true);
  } else if (node.Event == "SparkListenerBlockManagerRemoved") {
    push(node, 'block_manager');
  }
  if (events.length % 100000 == 0) {
    console.log("Got %d events", events.length);
  }
}).done(function(o) {
  var outFilename = process.argv.length >= 4 ? process.argv[3] : filename.replace(/\.json$/, ".idb");
  console.log("Got %d events; writing to file: ", events.length, outFilename);
  if (!appId) {
    console.error("App ID not found!");
  }
  var fd = fs.openSync(outFilename, 'w');
  fs.writeSync(fd, events.map(function(event) {
    event.tags.app = appId;
    return [
      [ event.key, flattenObj(event.tags, '=', ',')].join(','),
      'value=1',
      event.timestamp + '000000'
    ].join(' ')
  }).join('\n'));
}).fail(function(thrown, statusCode, body, jsonBody) {
  console.error("Fail:", arguments);
});
