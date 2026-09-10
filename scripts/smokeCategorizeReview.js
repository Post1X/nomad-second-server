/**
 * Smoke: description enrich → categorize (or Другое + is_active false).
 */
import mongoose from 'mongoose';
import dotenv from 'dotenv';
import { enrichEventDescriptions } from '../src/services/AiDescriptionServices';
import { categorizeNewEvent } from '../src/services/CategorizeEventServices';

dotenv.config();

async function main() {
  await mongoose.connect(process.env.MONGO_URI || process.env.MONGODB_URI || 'mongodb://127.0.0.1:27017', {
    dbName: process.env.DB_NAME || 'nomad_second',
  });

  const cats = await mongoose.connection.db.collection('eventscategories').find({}).project({ name: 1 }).toArray();
  const nameById = Object.fromEntries(cats.map((c) => [String(c._id), c.name]));

  const samples = [
    {
      name: 'Nordic Jazz Festival 2026',
      description: 'Three evenings of contemporary Nordic jazz with guest artists from Oslo and Helsinki.',
      address: 'Tallinn',
    },
    {
      name: 'Unknown Thing XYZ',
      description: '',
      address: '',
    },
    {
      name: 'VIP Upgrade',
      description: 'Package',
      address: '',
    },
  ];

  await enrichEventDescriptions(samples);

  const detailed = [];
  for (const s of samples) {
    // eslint-disable-next-line no-await-in-loop
    const { event, stats } = await categorizeNewEvent(s, 'ticketmaster');
    detailed.push({
      name: event.name,
      description: String(event.description || '').slice(0, 140),
      category: nameById[String(event.events_category_id)] || String(event.events_category_id),
      by: event.category_resolved_by,
      is_active: event.is_active,
      needs_manual_review: !!event.needs_manual_review,
      stats,
    });
  }

  console.log(JSON.stringify(detailed, null, 2));
  await mongoose.disconnect();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
