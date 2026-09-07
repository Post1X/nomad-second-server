import mongoose from 'mongoose';
import dotenv from 'dotenv';

dotenv.config();

async function main() {
  const uri = process.env.MONGO_URI || process.env.MONGODB_URI || 'mongodb://127.0.0.1:27017';
  await mongoose.connect(uri, { dbName: process.env.DB_NAME || 'nomad_second' });
  const db = mongoose.connection.db;
  console.log('connected', uri.replace(/\/\/.*@/, '//***@'));
  for (const n of ['parsedevents', 'cities', 'countries', 'eventscategories', 'parseruns']) {
    console.log(n, await db.collection(n).countDocuments());
  }
  const berlin = await db.collection('cities').find({
    $or: [
      { name: /berlin/i },
      { 'name_for_search': /berlin/i },
    ],
  }).limit(3).toArray();
  console.log('berlin sample', berlin.map((c) => ({ id: String(c._id), name: c.name })));
  await mongoose.disconnect();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
