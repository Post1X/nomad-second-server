import mongoose from 'mongoose';

const BackfillRunsSchema = new mongoose.Schema({
  purpose: {
    type: String,
    default: 'backfill',
    index: true,
  },
  status: {
    type: String,
    default: 'success',
  },
  /** Per-event outcomes for stats UI */
  results: {
    type: [{
      event_id: String,
      source: String,
      category_id: String,
      resolved_by: String,
      score: Number,
      city_id: String,
      country_id: String,
      enriched_description: Boolean,
      name: String,
      description: String,
      address: String,
      website: String,
      ticketmaster_id: String,
      holding_date: String,
      date_start: String,
      date_end: String,
      min_price: Number,
      max_price: Number,
      currency: String,
      specialization: String,
    }],
    default: [],
  },
  statistics: {
    type: mongoose.Schema.Types.Mixed,
    default: {},
  },
  openaiUsage: {
    type: mongoose.Schema.Types.Mixed,
    default: null,
  },
  meta: {
    type: mongoose.Schema.Types.Mixed,
    default: {},
  },
}, {
  timestamps: true,
});

BackfillRunsSchema.index({ createdAt: -1 });
BackfillRunsSchema.index({ 'results.source': 1, createdAt: -1 });

const BackfillRuns = mongoose.model('BackfillRuns', BackfillRunsSchema);

export default BackfillRuns;
