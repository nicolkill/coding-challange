const mongoose = require( "mongoose" );

const iterator = require('./iterator');

const host = process.env.MONGO_HOST;
const user = process.env.MONGO_USERNAME;
const password = process.env.MONGO_PASSWORD;

const LogSourceSchema = new mongoose.Schema({
  date: Date,
  msg: String
});

LogSourceSchema.index({
  date: 1
});

module.exports = class LogSourceAdmin {
  constructor(dbname) {
    const uri = `mongodb://${user}:${password}@${host}:27017/${dbname}?authSource=admin`;
    this.conn = mongoose.createConnection(uri, {useNewUrlParser: true, useUnifiedTopology: true});

    this.LogSource = this.conn.model('LogSource', LogSourceSchema)
  }
  async flush() {
    await this.LogSource.deleteMany({})
    this.conn.close();
  }
  // push({ date, msg }) {
  //   return this.LogSource.create({ date, msg });
  // }
  createModel({ date, msg }) {
    return new this.LogSource({ date, msg });
  }
  addMany(models) {
    return this.LogSource.insertMany(models);
  }
  async iterateLogs(printer) {
    const getLength = () => this.LogSource.countDocuments({});
    const query = (skip, limit) => this.LogSource.find({}, {}, {skip, limit, allowDiskUse: true}).sort({ date: 'asc' });
    const itemCall = (log) => printer.print(log);

    await iterator.iterate(getLength, query, itemCall);
  }
};
