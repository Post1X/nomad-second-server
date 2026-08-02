import mongoose from 'mongoose';
import { EVENT_SOURCE } from '../helpers/constants';

const SOURCES = Object.values(EVENT_SOURCE).filter((s) => s !== EVENT_SOURCE.nomad);

const CategorySuggestionsSchema = new mongoose.Schema({
  raw_name: {
    type: String,
    required: true,
  },
  normalized_key: {
    type: String,
    required: true,
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
  /** Rough OpenAI tokens attributed across events that suggested this name. */
  tokens_total: {
    type: Number,
    default: 0,
  },
  example_events: {
    type: [String],
    default: [],
  },
  sources: {
    type: [{
      type: String,
      enum: SOURCES,
    }],
    default: [],
  },
  first_seen_at: {
    type: Date,
    default: Date.now,
  },
  last_seen_at: {
    type: Date,
    default: Date.now,
  },
  reject_reason: {
    type: String,
    default: '',
  },
}, {
  timestamps: true,
});

CategorySuggestionsSchema.index({ normalized_key: 1 }, { unique: true });
CategorySuggestionsSchema.index({ status: 1, last_seen_at: -1 });

const CategorySuggestions = mongoose.model('CategorySuggestions', CategorySuggestionsSchema);

export default CategorySuggestions;
