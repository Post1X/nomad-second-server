import mongoose, { Schema } from 'mongoose';

const FientaPagesSchema = new mongoose.Schema({
  data: {
    type: String,
    required: true,
  },
  is_processed: {
    type: Boolean,
    default: false,
    index: true,
  },
  processed_at: {
    type: Date,
    required: false,
  },
  error_message: {
    type: String,
    default: '',
  },
}, {
  timestamps: true,
});

FientaPagesSchema.index({ is_processed: 1, createdAt: 1 });

const FientaPages = mongoose.model('FientaPages', FientaPagesSchema);

export default FientaPages;
