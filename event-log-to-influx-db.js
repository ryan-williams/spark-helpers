
var fs = require('fs');
var argv = require('minimist')(process.argv.slice(2));
var oboe = require('oboe');

var filename = argv._[0];
var is = fs.createReadStream(filename);

var appId = null;
var appName = null;

var stages = {};

var seconds = [];

var lastTime = null;
function getTime(node, timeKey) {
  var curTime;
  if (timeKey instanceof Array) {
    curTime = timeKey.reduce(function(o, segment) {
      return (segment in o) ? o[segment] : null;
    }, node);
  } else if (timeKey !== null) {
    curTime = node[timeKey || 'Timestamp'];
  }
  if (curTime) {
    curTime = parseInt(curTime / 1000);
    lastTime = curTime;
  } else {
    curTime = lastTime;
    if (timeKey !== null) {
      console.error("Missing time %s:", timeKey, node);
    }
  }
  return curTime;
}

function flattenObj(o, kvSep, fieldSep) {
  var r = [];
  o.app = appId;
  for (var k in o) {
    var v = o[k];
    if (typeof v === 'string') {
      v = escape(v);
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

function escape(s) {
  return s.toString().replace(/ /g, '\\ ');
}


function getSecond(t) {
  if (!(t in seconds)) {
    seconds[t] = {
      taskStarts: 0,
      taskEnds: {},
      executorAdds: 0,
      executorRemoves: {}
    };
  }
  return seconds[t];
}

function tag(k, v) {
  return k + '=' + escape(v);
}

console.log("Reading from:", filename);

var numEvents = 0;
var printInterval = setInterval(function() {
  console.log("Processed %d events", numEvents);
}, 1000);

oboe(is).node('events[*]', function(node) {
  numEvents++;
  if (node.Event == "SparkListenerApplicationStart") {
    appId = node["App ID"];
    appId = appId.substring(appId.length - 4);
    appName = node["App Name"];
    var t = getTime(node);
    var second = getSecond(t);
    second.apps = second.apps || [];
    second.apps.push({ id: appId, name: appName, start: true });
  } else if (node.Event == "SparkListenerApplicationEnd") {
    var t = getTime(node);
    var second = getSecond(t);
    second.apps = second.apps || [];
    second.apps.push({ id: appId, name: appName, start: false });
  } else if (node.Event == "SparkListenerJobStart") {
    var t = getTime(node, 'Submission Time');
    var second = getSecond(t);
    second.jobs = second.jobs || [];
    second.jobs.push({ id: node['Job ID'], start: true });
  } else if (node.Event == "SparkListenerJobEnd") {
    var t = getTime(node, 'Completion Time');
    var second = getSecond(t);
    second.jobs = second.jobs || [];
    second.jobs.push({
      id: node['Job ID'],
      success: node['Job Result']['Result'] == 'JobSucceeded',
      start: false
    });
  } else if (node.Event == "SparkListenerStageSubmitted") {

    var stageInfo = node['Stage Info'];
    var stageId = stageInfo['Stage ID'];
    var attemptId = stageInfo['Stage Attempt ID'];

    if (!(stageId in stages)) {
      stages[stageId] = {};
    }
    stages[stageId][attemptId] = { id: stageId + '.' + attemptId, start: true, time: lastTime };

  } else if (node.Event == "SparkListenerStageCompleted") {
    var stageInfo = node['Stage Info'];
    var stageId = stageInfo['Stage ID'];
    var attemptId = stageInfo['Stage Attempt ID'];

    var t = getTime(node, ['Stage Info', 'Completion Time']);
    var second = getSecond(t);
    second.stages = second.stages || [];
    second.stages.push({ id: stageId + '.' + attemptId, start: false });

    var submissionTime = getTime(node, ['Stage Info', 'Submission Time']);
    if (submissionTime) {
      var second = getSecond(submissionTime);
      second.stages = second.stages || [];
      var stage = stages[stageId][attemptId];
      delete stage.time;
      second.stages.push(stage);
      delete stages[stageId][attemptId];
      var isEmpty = true;
      for (var k in stages[stageId]) {
        isEmpty = false;
        break;
      }
      if (isEmpty) {
        delete stages[stageId];
      }
    } else {
      console.error("Missing submission time on StageCompletion event:", node);
    }

  } else if (node.Event == "SparkListenerTaskStart") {
    var t = getTime(node, ['Task Info', 'Launch Time']);
    var second = getSecond(t);
    second.taskStarts++;
  } else if (node.Event == "SparkListenerTaskEnd") {

    var t = getTime(node, ['Task Info', 'Finish Time']);
    var second = getSecond(t);

    var reason = node["Task End Reason"]["Reason"];
    var success = !node['Task Info']['Failed'];

    var key = tag('success', success);
    if (!success) {
      key += ',' + tag('reason', reason);
    }
    second.taskEnds[key] = (second.taskEnds[key] || 0) + 1;

  } else if (node.Event == "SparkListenerExecutorAdded") {
    var t = getTime(node);
    var second = getSecond(t);
    second.executorAdds++;
  } else if (node.Event == "SparkListenerExecutorRemoved") {
    var t = getTime(node);
    var second = getSecond(t);
    var key = tag('reason', node['Removed Reason']);
    second.executorRemoves[key] = (second.executorRemoves[key] || 0) + 1;
  }
}).done(function(o) {
  clearInterval(printInterval);

  for (var stageId in stages) {
    for (var attemptId in stages[stageId]) {
      var stage = stages[stageId][attemptId];
      console.log("Unfinished stage:", stage);
      var second = getSecond(stage.time);
      delete stage.time;
      second.stages = second.stages || [];
      second.stages.push(stage);
    }
  }

  var outFilename = argv._.length >= 2 ? argv._[1] : filename.replace(/\.json$/, ".idb");
  console.log("Got %d events; writing to file: ", numEvents, outFilename);
  if (!appId) {
    console.error("App ID not found!");
  }

  var numSeconds = 0;
  var secondsPrintInterval = setInterval(function() {
    console.log("Processed %d seconds", numSeconds);
  }, 1000);

  var fd = fs.openSync(outFilename, 'w');
  var stageTasksRunning = 0;

  for (var s in seconds) {
    function writeCount(k, key) {
      if (second[k]) {
        fs.writeSync(
              fd,
              [
                [key, flattenObj({}, '=', ',')].join(','),
                'value=' + second[k],
                s + '000000000'
              ].join(' ') + '\n'
        );
      }
    }

    function writeObj(k) {
      (second[k + 's'] || []).forEach(function(o) {
        fs.writeSync(
              fd,
              [
                [ k, flattenObj(o, '=', ',')].join(','),
                'value=1',
                s + '000000000'
              ].join(' ') + '\n'
        );
      });
    }

    function writeCounts(secondKey, influxKey) {
      var obj = second[secondKey];
      for (var k in obj) {
        var v = obj[k];
        if (v) {
          fs.writeSync(
                fd,
                [
                  [influxKey, k, flattenObj({}, '=', ',')].join(','),
                  'value=' + v,
                  s + '000000000'
                ].join(' ') + '\n'
          );
        }
      }
    }

    numSeconds++;
    var second = seconds[s];
    second.app = appId;
    writeObj('app');
    writeObj('job');
    writeObj('stage');

    writeCount('taskStarts', 'ts');
    writeCounts('taskEnds', 'te');
    writeCount('executorAdds', 'es');
    writeCounts('executorRemoves', 'ee');
  }
  console.log("done");
  clearInterval(secondsPrintInterval);
}).fail(function(e) {
  console.error("Oboe caught error:", e.thrown.stack);
});
