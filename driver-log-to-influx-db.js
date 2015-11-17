
var fs = require('fs');
var argv = require('minimist')(process.argv.slice(2));
var es = require('event-stream');
var moment = require('moment');

var instream = argv.length > 0 ? fs.createReadStream(argv._[0]) : process.stdin;

var stream = require( "stream" );
var util = require( "util" );

var outfile = argv.u || argv.unknown;
var outstream = null;
if (outfile) {
  outstream = fs.createWriteStream(outfile);
}

function LogLineParser() {
  // If this wasn't invoked with "new", return the newable instance.
  if ( ! ( this instanceof LogLineParser ) ) {
    return( new LogLineParser() );
  }
  stream.Transform.call(this, { objectMode: true });

  this._pattern = /^(\d\d\/\d\d\/\d\d \d\d:\d\d:\d\d) (INFO|WARN|ERROR) /;

  this._linesBuffer = [];
  this._dt = null;
  this._level = null;
}

util.inherits( LogLineParser, stream.Transform );

LogLineParser.prototype._flush = function( flushCompleted ) {
  var lastUnconsumedLine = 0;
  for (var i = 0; i < this._linesBuffer.length; i++) {
    var m = this._linesBuffer[i].match(this._pattern);
    if (m) {
      this._handleMatch(m, this._linesBuffer[i], lastUnconsumedLine, i);
      lastUnconsumedLine = i;
    }
  }
  if (!this._linesBuffer[this._linesBuffer.length - 1].trim()) {
    this._linesBuffer = this._linesBuffer.slice(0, this._linesBuffer.length - 1);
  }
  this.push({
    dt: this._dt,
    level: this._level,
    lines: this._linesBuffer.slice(lastUnconsumedLine)
  });

  this._linesBuffer = null;
  this.push( null );
  flushCompleted();
};

LogLineParser.prototype._handleMatch = function(m, line, from, to) {
  if (this._dt) {
    this.push({
      dt: this._dt,
      level: this._level,
      lines: this._linesBuffer.slice(from || 0, to || this._linesBuffer.length)
    });
  }
  this._dt = moment(m[1], 'YY/MM/DD hh:mm:ss');
  this._level = m[2];
  var lastIndex = m[0].length + m.index;
  this._linesBuffer = [ line.substr(lastIndex) ];
};

// I transform the given input chunk into zero or more output chunks.
LogLineParser.prototype._transform = function( buffer, encoding, getNextChunk ) {
  var line = buffer.toString('utf8');

  if (line.indexOf('\n') >= 0) {
    console.error('more than one line in chunk:', line);
  }

  var m = line.match(this._pattern);
  if (m) {
    this._handleMatch(m, line);
  } else {
    this._linesBuffer.push(line);
  }
  getNextChunk();
};

var seconds = [];
var prevTime = null;

var fields = {
  AddedBroadcast: 'ab',
  AttemptedToMarkUnknownExecutorIdle: 'amuei',
  AddedRdd: 'ardd',
  AskedRemoveNonexistentExecutor: 'arnee',
  YarnSchedulerAddingTaskSet: 'ats',
  BlockBroadcastStored: 'bbs',
  TryingToRemoveExecutor: 'bmetre',
  BlockManagerRemovedSuccessfully: 'bmmr',
  BlockManagerMasterStopped: 'bmms',
  BlockManagerRemovingRdd: 'bmrrdd',
  BlockManagerStopped: 'bms',
  ContextHandlerStopped: 'chs',
  DAGExecutorLost: 'dagel',
  DAGSchedulerFinalStage: 'dsfns',
  DAGSchedulerFailedSet: 'dsfs',
  DAGSchedulerGotJob: 'dsgj',
  DAGSchedulerJobFailed: 'dsjf',
  DAGSchedulerJobFinished: 'dsjp',
  DroppingSparkListenerEvent: 'dsle',
  DAGSchedulerLookingForRunnableStages: 'dslrs',
  DAGSchedulerMissingParents: 'dsmp',
  DAGSchedulerParentsOfFinalStage: 'dspfs',
  DAGSchedulerRunningSet: 'dsrs',
  DAGSchedulerStageFailed: 'dssf',
  DAGSchedulerStageFinished: 'dssp',
  DAGSchedulerSubmittingStage: 'dsss',
  DAGSchedulerSubmittingTasks: 'dsst',
  DAGSchedulerWaitingSet: 'dsws',
  ExistingExecutorRemoved: 'eer',
  EnsureFreeSpace: 'efs',
  LostExecutor: 'elost',
  FinishedTask: 'end',
  ErrorNotifyingEndpoint: 'ene',
  AkkaErrorSendingMessage: 'esm',
  InterruptingMonitorThread: 'imt',
  LostTask: 'lost',
  MapOutputLocations: 'mols',
  MapOutputTrackerStopped: 'mots',
  MemoryStoreCleared: 'msc',
  MarkingTaskSpeculatable: 'mts',
  Messages: 'ms',
  NewExecutor: 'ne',
  RemovingExecutorNoRecentHeartbeats: 'nrhbs',
  OutputCommitCoordinatorStopped: 'occs',
  RegisteringBlockManager: 'rbm',
  AssociationWithRemoteSystemFailed: 'rdsf',
  RegisteredExecutor: 're',
  RegisteringRDD: 'rgrdd',
  RemovingIdleExecutor: 'rie',
  RequestingToKillExecutor: 'rke',
  RemovingBlockManager: 'rmbm',
  RemovingRdd: 'rmrdd',
  RequestingNewExecutors: 'rne',
  RequeueingTasks: 'rqt',
  RemovedRddBlock: 'rrdd',
  SparkContextCreatedBroadcast: 'sccb',
  SparkContextInvokingStop: 'scis',
  SparkContextStopped: 'scs',
  ShuttingDownAllExecutors: 'sdae',
  StoppingDAGScheduler: 'sds',
  ShutdownHookCalled: 'shc',
  ShutdownHookDeletingDirectory: 'shdd',
  StartingJob: 'sj',
  SparkListenerBusAlreadyStopped: 'slbas',
  ShutdownMessages: 'sm',
  SparkContextAlreadyStopped: 'spas',
  StartingTask: 'start',
  StoppedWebUI: 'swui',
  TaskDeniedCommitting: 'tdc',
  TotalInputPaths: 'tip',
  TaskSetFinished: 'tss',
  UncaughtException: 'ue',
  Unknown: 'unknown'
};

var fieldsInv = {};
for (var k in fields) {
  var v = fields[k];
  if (v in fieldsInv) {
    throw new Error("Key " + v + " found in fields more than once: " + fieldsInv[v] + "," + k);
  }
  fieldsInv[v] = k;
}

function aggBySecond(obj, cb) {
  var curTime = obj.dt.unix();
  if (curTime != prevTime && seconds[prevTime]) {
    cb(null, seconds[prevTime]);
  }

  function inc(key) {
    if (!(curTime in seconds)) {
      seconds[curTime] = { dt: curTime };
    }
    var second = seconds[curTime];
    var tags = [];
    var args = Array.prototype.slice.call(arguments, 1);
    args.forEach(function(arg) {
      if (typeof arg === 'string') {
        if (arg.indexOf('=') < 0) {
          tags.push([arg, 1].join('='));
        } else {
          tags.push(arg);
        }
      } else if (arg instanceof Array) {
        tags.push(arg.join('='));
      } else {
        throw new Error("Invalid tag: " + arg);
      }
    });
    tags.sort();
    var tagsStr = tags.join(',');
    if (!(tagsStr in second)) {
      second[tagsStr] = {};
    }
    second[tagsStr][key] = (second[tagsStr][key] || 0) + 1;
  }

  inc(fields.Messages, [ 'level', obj.level ]);

  var line = obj.lines[0];
  if (line.match(/^scheduler\.TaskSetManager: Starting task/)) {
    inc(fields.StartingTask);
  } else if (line.match(/^scheduler\.TaskSetManager: Finished task/)) {
    inc(fields.FinishedTask);
  } else if (line.match(/^scheduler\.TaskSetManager: Lost task/)) {
    var reason = 'unk';
    if (line.match(/ExecutorLostFailure/)) {
      reason = 'elf';
    } else if (line.match(/ClosedChannelException/)) {
      reason = 'cce';
    } else if (line.match(/java\.io\.IOException:? \(?Filesystem closed\)?/)) {
      reason = 'fsc';
    } else if (line.match(/TaskCommitDenied/)) {
      reason = 'tcd';
    } else if (line.match(/java\.io\.IOException:? \(?Failed to connect to/)) {
      reason = 'ftc';
    }
    inc(fields.LostTask, [ 'reason', reason ]);
  } else if (line.match(/^cluster\.YarnScheduler: Lost (?:an )?executor/)) {
    var reason = 'unk';
    if (line.match(/Executor heartbeat timed out after/)) {
      reason = 'ehbto';
    } else if (line.match(/remote Rpc client disassociated/)) {
      reason = 'rrcd';
    } else if (line.match(/Yarn deallocated the executor/)) {
      reason = 'ydte';
    }
    inc(fields.LostExecutor, [ 'reason', reason ]);
  } else if (line.match(/^akka\.AkkaRpcEndpointRef: Error sending message/)) {
    var reason = 'unk';
    if (obj.lines[1].match(/^org\.apache\.spark\.rpc\.RpcTimeoutException: Futures timed out after/)) {
      reason = 'fto';
    }
    var msg = 'unk';
    var m = line.match(/\[message = (\w+)/);
    if (m) {
      msg = m[1];
    }
    inc(fields.AkkaErrorSendingMessage, [ 'reason', reason ], [ 'msg', msg ]);
  } else if (line.match(/^remote\.ReliableDeliverySupervisor: Association with remote system/)) {
    inc(fields.AssociationWithRemoteSystemFailed);
  } else if (line.match(/^storage\.BlockManagerInfo: Added broadcast_/)) {
    inc(fields.AddedBroadcast);
  } else if (line.match(/^storage\.BlockManagerInfo: Added rdd_/)) {
    inc(fields.AddedRdd);
  } else if (line.match(/^storage\.BlockManagerInfo: Removed rdd_/)) {
    inc(fields.RemovedRddBlock);
  } else if (line.match(/^cluster\.YarnClientSchedulerBackend: Registered executor/)) {
    inc(fields.RegisteredExecutor);
  } else if (line.match(/^storage\.BlockManagerMasterEndpoint: Registering block manager/)) {
    inc(fields.RegisteringBlockManager);
  } else if (line.match(/^spark\.ExecutorAllocationManager: New executor/)) {
    inc(fields.NewExecutor);
  } else if (line.match(/^spark.MapOutputTrackerMasterEndpoint: Asked to send map output locations for shuffle/)) {
    inc(fields.MapOutputLocations);
  } else if (line.match(/^scheduler\.DAGScheduler: Executor lost/)) {
    inc(fields.DAGExecutorLost);
  } else if (line.match(/^storage\.BlockManagerMasterEndpoint: Trying to remove executor/)) {
    inc(fields.TryingToRemoveExecutor);
  } else if (line.match(/^storage\.BlockManagerMasterEndpoint: Removing block manager/)) {
    inc(fields.RemovingBlockManager);
  } else if (line.match(/^storage\.BlockManagerMaster: Removed /)) {
    inc(fields.BlockManagerRemovedSuccessfully);
  } else if (line.match(/^cluster\.YarnClientSchedulerBackend: Asked to remove non-existent executor/)) {
    inc(fields.AskedRemoveNonexistentExecutor);
  } else if (line.match(/^scheduler\.TaskSetManager: Re-queueing tasks for/)) {
    inc(fields.RequeueingTasks);
  } else if (line.match(/^spark\.ExecutorAllocationManager: Existing executor \d+ has been removed \(new total is (\d+)\)/)) {
    inc(fields.ExistingExecutorRemoved);
  } else if (line.match(/^spark\.ExecutorAllocationManager: Requesting/)) {
    inc(fields.RequestingNewExecutors);
  } else if (line.match(/^scheduler\.LiveListenerBus: Dropping SparkListenerEvent/)) {
    inc(fields.DroppingSparkListenerEvent);
  } else if (line.match(/^spark\.HeartbeatReceiver: Removing executor/)) {
    inc(fields.RemovingExecutorNoRecentHeartbeats);
  } else if (line.match(/^handler\.ContextHandler: stopped/)) {
    inc(fields.ContextHandlerStopped);
  } else if (line.match(/^cluster\.YarnClientSchedulerBackend: Requesting to kill executor/)) {
    inc(fields.RequestingToKillExecutor);
  } else if (line.match(/^scheduler\.TaskSetManager: Marking task/)) {
    inc(fields.MarkingTaskSpeculatable);
  } else if (line.match(/^scheduler\.OutputCommitCoordinator: Task was denied committing/)) {
    inc(fields.TaskDeniedCommitting);
  } else if (line.match(/^spark\.ExecutorAllocationManager: Removing executor (?:\d+) because it has been idle/)) {
    inc(fields.RemovingIdleExecutor);
  } else if (line.match(/^spark\.ExecutorAllocationManager: Attempted to mark unknown executor/)) {
    inc(fields.AttemptedToMarkUnknownExecutorIdle);
  } else if (line.match(/^akka\.AkkaRpcEnv: Ignore error: Error notifying standalone scheduler's driver endpoint/)) {
    inc(fields.ErrorNotifyingEndpoint);
  } else if (line.match(/: Uncaught exception in thread/)) {
    inc(fields.UncaughtException);
  } else if (line.match(/^spark\.SparkContext: SparkContext already stopped/)) {
    inc(fields.SparkContextAlreadyStopped);
    inc(fields.ShutdownMessages);
  } else if (line.match(/^scheduler\.DAGScheduler: Stopping DAGScheduler/)) {
    inc(fields.StoppingDAGScheduler);
    inc(fields.ShutdownMessages);
  } else if (line.match(/^spark\.SparkContext: Invoking stop\(\) from shutdown hook/)) {
    inc(fields.SparkContextInvokingStop);
    inc(fields.ShutdownMessages);
  } else if (line.match(/^ui\.SparkUI: Stopped Spark web UI at/)) {
    inc(fields.StoppedWebUI);
    inc(fields.ShutdownMessages);
  } else if (line.match(/^cluster\.YarnClientSchedulerBackend: Shutting down all executors/)) {
    inc(fields.ShuttingDownAllExecutors);
    inc(fields.ShutdownMessages);
  } else if (line.match(/^cluster\.YarnClientSchedulerBackend: Interrupting monitor thread/)) {
    inc(fields.InterruptingMonitorThread);
    inc(fields.ShutdownMessages);
  } else if (line.match(/^storage\.MemoryStore: Block broadcast_/)) {
    inc(fields.BlockBroadcastStored);
  } else if (line.match(/^storage\.MemoryStore: ensureFreeSpace/)) {
    inc(fields.EnsureFreeSpace);
  } else if (line.match(/^scheduler\.DAGScheduler: Registering RDD/)) {
    inc(fields.RegisteringRDD);
  } else if (line.match(/^spark\.SparkContext: Starting job:/)) {
    inc(fields.StartingJob);
  } else if (line.match(/^mapred\.FileInputFormat: Total input paths to process/)) {
    inc(fields.TotalInputPaths);
  } else if (line.match(/^scheduler\.DAGScheduler: Got job/)) {
    inc(fields.DAGSchedulerGotJob);
  } else if (line.match(/^scheduler\.DAGScheduler: Final stage:/)) {
    inc(fields.DAGSchedulerFinalStage);
  } else if (line.match(/^scheduler\.DAGScheduler: Parents of final stage:/)) {
    inc(fields.DAGSchedulerParentsOfFinalStage);
  } else if (line.match(/^scheduler\.DAGScheduler: Missing parents:? /)) {
    inc(fields.DAGSchedulerMissingParents);
  } else if (line.match(/^scheduler\.DAGScheduler: Submitting .*Stage .*, which (?:has no missing parents|is now runnable)/)) {
    inc(fields.DAGSchedulerSubmittingStage);
  } else if (line.match(/^spark\.SparkContext: Created broadcast/)) {
    inc(fields.SparkContextCreatedBroadcast);
  } else if (line.match(/^scheduler\.DAGScheduler: Submitting \d+ missing tasks/)) {
    inc(fields.DAGSchedulerSubmittingTasks);
  } else if (line.match(/^cluster\.YarnScheduler: Adding task set/)) {
    inc(fields.YarnSchedulerAddingTaskSet);
  } else if (line.match(/^rdd\.(\w)+RDD: Removing RDD \d+ from persistence list/)) {
    inc(fields.RemovingRdd);
  } else if (line.match(/^storage\.BlockManager: Removing RDD/)) {
    inc(fields.BlockManagerRemovingRdd);
  } else if (line.match(/^scheduler\.LiveListenerBus: SparkListenerBus has already stopped/)) {
    inc(fields.SparkListenerBusAlreadyStopped);
  } else if (line.match(/^scheduler\.DAGScheduler: Job \d+ failed:/)) {
    inc(fields.DAGSchedulerJobFailed);
  } else if (line.match(/^scheduler\.DAGScheduler: .*Stage .* failed in /)) {
    inc(fields.DAGSchedulerStageFailed);
  } else if (line.match(/^scheduler.DAGScheduler: looking for newly runnable stages/)) {
    inc(fields.DAGSchedulerLookingForRunnableStages);
  } else if (line.match(/^scheduler.DAGScheduler: running: Set/)) {
    inc(fields.DAGSchedulerRunningSet);
  } else if (line.match(/^scheduler.DAGScheduler: waiting: Set/)) {
    inc(fields.DAGSchedulerWaitingSet);
  } else if (line.match(/^scheduler.DAGScheduler: failed: Set/)) {
    inc(fields.DAGSchedulerFailedSet);
  } else if (line.match(/^cluster\.YarnScheduler: Removed TaskSet [\d.]+, whose tasks have all completed, from pool/)) {
    inc(fields.TaskSetFinished);
  } else if (line.match(/^scheduler\.DAGScheduler: Job \d+ finished:/)) {
    inc(fields.DAGSchedulerJobFinished);
  } else if (line.match(/^scheduler\.DAGScheduler: .*Stage .* finished in /)) {
    inc(fields.DAGSchedulerStageFinished);
  } else if (line.match(/^storage\.MemoryStore: MemoryStore cleared/)) {
    inc(fields.MemoryStoreCleared);
    inc(fields.ShutdownMessages);
  } else if (line.match(/^storage\.BlockManager: BlockManager stopped/)) {
    inc(fields.BlockManagerStopped);
    inc(fields.ShutdownMessages);
  } else if (line.match(/^storage\.BlockManagerMaster: BlockManagerMaster stopped/)) {
    inc(fields.BlockManagerMasterStopped);
    inc(fields.ShutdownMessages);
  } else if (line.match(/^spark\.SparkContext: Successfully stopped SparkContext/)) {
    inc(fields.SparkContextStopped);
    inc(fields.ShutdownMessages);
  } else if (line.match(/^scheduler\.OutputCommitCoordinator.*: OutputCommitCoordinator stopped/)) {
    inc(fields.OutputCommitCoordinatorStopped);
    inc(fields.ShutdownMessages);
  } else if (line.match(/^util\.ShutdownHookManager: Shutdown hook called/)) {
    inc(fields.ShutdownHookCalled);
    inc(fields.ShutdownMessages);
  } else if (line.match(/^util\.ShutdownHookManager: Deleting directory/)) {
    inc(fields.ShutdownHookDeletingDirectory);
    inc(fields.ShutdownMessages);
  } else if (line.match(/^spark\.MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped/)) {
    inc(fields.MapOutputTrackerStopped);
    inc(fields.ShutdownMessages);
  //} else if (line.match(/^/)) {
  //  inc(fields.);
  //} else if (line.match(/^/)) {
  //  inc(fields.);
  //} else if (line.match(/^/)) {
  //  inc(fields.);
  } else {
    inc(fields.Unknown, ['level', obj.level]);
    if (outstream) {
      outstream.write([ obj.dt.format('YY/MM/DD HH:mm:ss'), obj.level, obj.lines.join('\n') ].join(' ') + '\n');
    }
  }
  prevTime = curTime;
}

instream
      .pipe(es.split())
      .pipe(new LogLineParser())
      .pipe(es.through(
            function(obj) {
              aggBySecond(
                    obj,
                    function(err, val) {
                      this.emit('data', val);
                    }.bind(this)
              );
            },
            function() {
              if (prevTime) {
                this.emit('data', seconds[prevTime]);
              }
              this.emit('end');
            }
      ))
      .pipe(es.through(
            function(obj) {
              for (var tagsStr in obj) {
                if (tagsStr == 'dt') continue;
                var o = obj[tagsStr];
                var arr = [];
                for (var k in o) {
                  if (o[k]) {
                    arr.push([k, o[k]].join('='));
                  }
                }
                var fields = arr.join(',');
                var key = ['dl'].concat(tagsStr ? [tagsStr] : []).join(',');
                this.emit('data', [key, fields, obj.dt + '000000000'].join(' ') + '\n');
              }
            }
      ))
      .pipe(process.stdout);
