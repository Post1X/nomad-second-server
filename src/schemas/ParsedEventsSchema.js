import mongoose, { Schema } from 'mongoose';
import { EVENT_SOURCE } from '../helpers/constants';

const ParsedEventsSchema = new mongoose.Schema({
  /** Winning source after priority merge. */
  source: {
    type: String,
    enum: Object.values(EVENT_SOURCE).filter((s) => s !== EVENT_SOURCE.nomad),
    required: true,
    index: true,
  },
  /** Normalized name for identity lookup (with city_id). */
  name_key: {
    type: String,
    required: true,
    index: true,
  },
  /** City id string — part of identity with name_key. */
  city_id: {
    type: String,
    required: true,
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
}, {
  timestamps: true,
});

ParsedEventsSchema.index(
  { parser_unique_id: 1 },
  { unique: true, partialFilterExpression: { parser_unique_id: { $type: 'string' } } },
);
ParsedEventsSchema.index(
  { name_key: 1, city_id: 1 },
  { unique: true },
);
ParsedEventsSchema.index({ source: 1, updatedAt: 1 });

const ParsedEvents = mongoose.model('ParsedEvents', ParsedEventsSchema);

export default ParsedEvents;
