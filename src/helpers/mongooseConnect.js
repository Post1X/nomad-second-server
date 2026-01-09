import mongoose from 'mongoose';

const { DB_NAME } = process.env;

mongoose.set('strictQuery', false);

export default async function (cb) {
  try {
    await mongoose.connect(`mongodb://localhost:27017/${DB_NAME}`);

    console.log('Connected to db');

    if (typeof cb === 'function') {
      cb();
    }
  } catch (e) {
    console.warn(e);
  }
}

