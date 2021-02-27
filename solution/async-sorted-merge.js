"use strict";

const {
  Worker,
  isMainThread ,
  parentPort,
  workerData: logs
} = require('worker_threads');

const iterator = require('./iterator');
const LogSourceAdmin = require('./log-source-admin');

const logSourceAdmin = new LogSourceAdmin('async');

if (!isMainThread) {
  const exec = async (logs) => {
    const models = logs.map((log) => logSourceAdmin.createModel(log));
    await logSourceAdmin.addMany(models)
    parentPort.postMessage(true);
  };

  exec(logs);
}

// Print all entries, across all of the *async* sources, in chronological order.

module.exports = (logSources, printer) => {
  return new Promise(async (resolve, reject) => {
    const promises = [];
    const getLength = () => logSources.length;
    const query = async (_, limit) => {
      const logs = await Promise.all(logSources.splice(0, limit).map((log) => log.popAsync()));
      promises.push(new Promise(async (resolve) => {
        const worker = new Worker(__filename, {
          workerData: logs
        });
        worker.on('message', () => {
          worker.terminate();
          resolve();
        });
      }));
      if (promises.length >= 10) {
        await Promise.all(promises);
        promises.splice(0, 10)
      }
      return [];
    }
    await iterator.iterate(getLength, query, null, 10000);
    await Promise.all(promises);

    await logSourceAdmin.iterateLogs(printer);
    printer.done();

    await logSourceAdmin.flush();

    resolve(console.log("Async sort complete."));
  });
};
