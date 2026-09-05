import mongoose, { Schema } from 'mongoose';
import { EVENT_SOURCE } from '../helpers/constants';

const SOURCES = Object.values(EVENT_SOURCE).filter((s) => s !== EVENT_SOURCE.nomad);

const CitySuggestionsSchema = new mongoose.Schema({
  source: {
    type: String,
    enum: SOURCES,
    required: true,
    index: true,
  },
  raw_name: {
    type: String,
    required: true,
  },
  normalized_key: {
    type: String,
    required: true,
    index: true,
  },
  slug: {
    type: String,
    default: '',
  },
  source_url: {
    type: String,
    default: '',
  },
  status: {
    type: String,
    enum: ['pending', 'rejected'],
    default: 'pending',
    index: true,
  },
  hit_count: {
    type: Number,
    default: 1,
  },
  first_seen_at: {
    type: Date,
    default: Date.now,
  },
  last_seen_at: {
    type: Date,
    default: Date.now,
  },
  possible_duplicate_of: {
    type: Schema.Types.ObjectId,
    ref: 'Cities',
    required: false,
  },
  possible_duplicate_name: {
    type: String,
    default: '',
  },
  reject_reason: {
    type: String,
    default: '',
  },
}, {
  timestamps: true,
});

CitySuggestionsSchema.index({ source: 1, normalized_key: 1 }, { unique: true });
CitySuggestionsSchema.index({ status: 1, source: 1, last_seen_at: -1 });

const CitySuggestions = mongoose.model('CitySuggestions', CitySuggestionsSchema);

export default CitySuggestions;
