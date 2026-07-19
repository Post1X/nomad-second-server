import mongoose, { Schema } from 'mongoose';
import { EVENT_SOURCE } from '../helpers/constants';

const ParsedEventsSchema = new mongoose.Schema({
  source: {
    type: String,
    enum: Object.values(EVENT_SOURCE).filter((s) => s !== EVENT_SOURCE.nomad),
    required: true,
    index: true,
  },
  fingerprint: {
    type: String,
    required: true,
    index: true,
  },
  event_data: {
    type: Schema.Types.Mixed,
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

ParsedEventsSchema.index({ source: 1, fingerprint: 1 }, { unique: true });
ParsedEventsSchema.index({ source: 1, updatedAt: 1 });
ParsedEventsSchema.index({ source: 1, exported_at: 1, updatedAt: 1 });

const ParsedEvents = mongoose.model('ParsedEvents', ParsedEventsSchema);

export default ParsedEvents;
