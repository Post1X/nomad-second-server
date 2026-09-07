/**
 * Smoke test: AI description enrich (no DB writes, no deploy).
 * Usage: yarn babel-node -r dotenv/config scripts/smokeAiDescription.js
 */
import { enrichEventDescriptions } from '../src/services/AiDescriptionServices';

async function main() {
  const samples = [
    {
      name: 'CASCADA',
      description: '',
      address: 'Berlin, Mercedes-Benz Arena',
    },
    {
      name: 'Stand-up Night',
      description: 'Stand-up Night',
      address: 'Riga',
    },
    {
      name: 'Александр Васильев в Берлине. "Секс и мода"',
      description: '',
      address: 'Berlin',
    },
    {
      name: 'Балет "Лебединое озеро". Classico Ballet Napoli 2026-2027',
      description: 'Classico Ballet Napoli',
      address: 'Amberg, Congress Centrum',
    },
    {
      name: 'The Beatles Tribute Show в Германии 2026',
      description: 'С королевской рекомендацией:',
      address: 'Dresden, Parkhotel Ballsaal',
    },
    {
      name: 'Nordic Jazz Festival 2026',
      description: 'Three evenings of contemporary Nordic jazz with guest artists from Oslo and Helsinki. Doors 19:00.',
      address: 'Tallinn, Estonia',
    },
    {
      name: 'VIP Upgrade',
      description: 'Package',
      address: '',
    },
    {
      name: 'Unknown Thing XYZ',
      description: '',
      address: '',
    },
  ];

  const snapshot = samples.map((s) => ({
    name: s.name,
    description: s.description,
  }));

  const { events, stats } = await enrichEventDescriptions(samples);

  console.log('STATS', JSON.stringify(stats, null, 2));
  console.log('OUTPUT', JSON.stringify(events.map((e, i) => ({
    name: e.name,
    before: snapshot[i].description,
    after: e.description,
    changed: e.description !== snapshot[i].description,
    description_resolved_by: e.description_resolved_by || null,
    needs_manual_review: e.needs_manual_review,
    description_ai_failed: e.description_ai_failed,
  })), null, 2));
}

main().catch((e) => {
  console.error('FAIL', e?.message || e);
  process.exit(1);
});
