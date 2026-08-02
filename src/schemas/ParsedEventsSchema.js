import mongoose, { Schema } from 'mongoose';
import { EVENT_SOURCE } from '../helpers/constants';

const ParsedEventsSchema = new mongoose.Schema({
  /** Winning source after cross-source merge (SOURCE_PRIORITY). */
  source: {
    type: String,
    enum: Object.values(EVENT_SOURCE).filter((s) => s !== EVENT_SOURCE.nomad),
    required: true,
    index: true,
  },
  /** Global identity: sha256(name + city_id) — unique across all sources. */
  fingerprint: {
    type: String,
    required: true,
    unique: true,
    index: true,
  },
  /** Stable id for main pull create/update (also copied into event_data). */
  parser_unique_id: {
    type: String,
    required: false,
  },
  event_data: {
    type: mongoose.Schema.Types.Mixed,
    required: true,
  },
  parse_run: {
    type: Schema.Types.ObjectId,
    ref: 'ParseRuns',
    required: false,
    index: true,
  },
  exported_at: {
    type: Date,
    default: null,
    index: true,
  },
}, {
  timestamps: true,
});

ParsedEventsSchema.index(
  { parser_unique_id: 1 },
  { unique: true, partialFilterExpression: { parser_unique_id: { $type: 'string' } } },
);
ParsedEventsSchema.index({ source: 1, updatedAt: 1 });
ParsedEventsSchema.index({ source: 1, exported_at: 1, updatedAt: 1 });

const ParsedEvents = mongoose.model('ParsedEvents', ParsedEventsSchema);

export default ParsedEvents;
