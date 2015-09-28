# spark-helpers
Scripts for fetching and caching Spark builds, by version number or git SHA.

## Installing

- `source .spark-rc` in your `.bashrc` (or equivalent)
- put the path to this repository on your `$PATH`

By default, clones of the Spark repository, and their built artifacts, will be placed in `$HOME/sparks`, and the `$sparks` env var set to that directory. Feel free to set your own value of `$sparks` prior to `source`ing `.spark-rc`, and that will be used instead.

## Usage

Clone, build, and select a given Spark SHA:

```
$ spark-select <sha> [hadoop version]
```

When this finishes, you'll have a version of Spark built from `<sha>` for Hadoop `[hadoop version]` at `$HOME/sparks/spark-<sha>-bin-hadoop[hadoop version]`, with `$SPARK_HOME` pointing at it.

### Example

```
$ spark-select 29ace3b
  Cloning spark into /Users/ryan/sparks/spark-29ace3b-bin-hadoop2.4
  …
  Note: checking out '29ace3b'.
  …
  Cmd: build/mvn -Pyarn -Phadoop-2.4 -Dhadoop.version=2.4.0 -DskipTests -DzincPort=3031  package
  exec: curl --progress-bar -L http://downloads.typesafe.com/zinc/0.3.5.3/zinc-0.3.5.3.tgz
  ######################################################################## 100.0%
  exec: curl --progress-bar -L http://downloads.typesafe.com/scala/2.10.4/scala-2.10.4.tgz
  ######################################################################## 100.0%
  Using `mvn` from path: /usr/local/bin/mvn
  [INFO] Scanning for projects...
  …
  [INFO] BUILD SUCCESS
  [INFO] ------------------------------------------------------------------------
  [INFO] Total time: 16:07 min
  [INFO] Finished at: 2015-08-06T14:57:55+00:00
  [INFO] Final Memory: 69M/336M
  [INFO] ------------------------------------------------------------------------
$ echo $SPARK_HOME
  /Users/ryan/sparks/spark-29ace3b-bin-hadoop2.4
```

(See [this gist](https://gist.github.com/ryan-williams/f79b108b7ab52f5f398a) for full example output).

### Setting History Server Opts
This repo includes a helper, `spark-history-opts`, for changing the `$SPARK_HISTORY_OPTS` env var:

```
$ spark-history-opts 12345 /path/to/dir 10000
$ echo $SPARK_HISTORY_OPTS
-Dspark.history.ui.port=12345 -Dspark.history.fs.logDirectory=/path/to/dir -Dspark.history.retainedApplications=10000

# Start Spark history server
$ $SPARK_HOME/sbin/start-history-server.sh

# Omitted values default to 18080, the contents of the $SPARK_EVENTLOG_DIR env var, and 1000
$ spark-history-opts
$ echo $SPARK_HISTORY_OPTS
-Dspark.history.ui.port=18080 -Dspark.history.fs.logDirectory=… -Dspark.history.retainedApplications=5000
```

### Notes
* `<Hadoop version>` [defaults to `2.4`, or whatever you've set `$SPARK_HELPERS_HADOOP_VERSION` to](https://github.com/ryan-williams/spark-helpers/blob/96026b95edeffdcc3f40549db64e42f4d1f7ff78/.spark-rc#L21).
* The environment variable `$SPARK_BUILD_ARGS` [allows passing extra arguments to the `mvn package` command that builds Spark](https://github.com/ryan-williams/spark-helpers/blob/96026b95edeffdcc3f40549db64e42f4d1f7ff78/spark-build#L50); e.g. you may want to build a certain profile:

  ```
  export SPARK_BUILD_ARGS="-Pyarn"
  ```

* By default, [`spark-clone`](https://github.com/ryan-williams/spark-helpers/blob/master/spark-clone) will clone `git@github.com:apache/spark.git`; set your own `$SPARK_REPO_URL` env var if you want to use e.g. a local Spark clone you are managing separately. This can be useful if you have work in a local clone that you are interested in building/selecting via `spark-select`.

* After you've cloned and built a {sha, hadoop-version} Spark, `spark-select` will reuse it if you run it again with those parameters.
* [`spark-build`](https://github.com/ryan-williams/spark-helpers/blob/96026b95edeffdcc3f40549db64e42f4d1f7ff78/spark-build) ([called by `spark-select`](https://github.com/ryan-williams/spark-helpers/blob/96026b95edeffdcc3f40549db64e42f4d1f7ff78/spark-select-impl#L16)) [attempts to find a free `$ZINC_PORT` to use](https://github.com/ryan-williams/spark-helpers/blob/96026b95edeffdcc3f40549db64e42f4d1f7ff78/spark-build#L32-L47), allowing multiple builds to occur simultaneously and all use [`zinc`](https://github.com/typesafehub/zinc).
* If the `<sha>` passed is a valid Spark version, e.g. `1.5.0`, `spark-helpers` will fetch the relevant release from a Spark mirror of your choosing (default: http://mirrors.advancedhosters.com/apache; set `$SPARK_MIRROR` to change). For example:

  ```
  $ spark-select 1.5.0
  ```
