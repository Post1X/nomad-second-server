import mongoose, { Schema } from 'mongoose';
import { EVENT_SOURCE, OPERATION_STATUSES } from '../helpers/constants';

const ParseRunsSchema = new mongoose.Schema({
  source: {
    type: String,
    enum: Object.values(EVENT_SOURCE).filter((s) => s !== EVENT_SOURCE.nomad),
    required: true,
    index: true,
  },
  status: {
    type: String,
    enum: Object.values(OPERATION_STATUSES),
    required: true,
    default: OPERATION_STATUSES.pending,
  },
  statistics: {
    type: String,
    default: '',
  },
  errorText: {
    type: String,
    default: '',
  },
  infoText: {
    type: String,
    default: '',
  },
  meta: {
    type: Schema.Types.Mixed,
    default: {},
  },
  startedAt: {
    type: Date,
    default: Date.now,
  },
  finishedAt: {
    type: Date,
  },
  cancelRequested: {
    type: Boolean,
    default: false,
  },
}, {
  timestamps: true,
});

ParseRunsSchema.index({ source: 1, createdAt: -1 });
ParseRunsSchema.index({ source: 1, status: 1, finishedAt: -1 });

const ParseRuns = mongoose.model('ParseRuns', ParseRunsSchema);

export default ParseRuns;
