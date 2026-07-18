import mongoose, { Schema } from 'mongoose';
import { OPERATION_STATUSES, OPERATION_TYPES } from '../helpers/constants';

const OperationsSchema = new mongoose.Schema({
  type: {
    type: String,
    enum: Object.values(OPERATION_TYPES),
    required: true,
  },
  admin: {
    type: Schema.Types.ObjectId,
    required: false,
    ref: 'Admins',
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
  finish_time: {
    type: Date,
  },
  is_processed: {
    type: Boolean,
    default: false,
  },
  is_taken: {
    type: Boolean,
    default: false,
  },
  taken_at: {
    type: Date,
    required: false,
  },
}, {
  timestamps: true,
});

OperationsSchema.index({ is_taken: 1, taken_at: 1 });

const Operations = mongoose.model('Operations', OperationsSchema);

export default Operations;

