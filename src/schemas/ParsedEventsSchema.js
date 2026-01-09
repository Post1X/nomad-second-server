import mongoose, { Schema } from 'mongoose';

const ParsedEventsSchema = new mongoose.Schema({
  operation: {
    type: Schema.Types.ObjectId,
    ref: 'Operations',
    required: true,
    index: true,
  },
  event_data: {
    type: Schema.Types.Mixed,
    required: true,
  },
  batch_number: {
    type: Number,
    required: true,
  },
}, {
  timestamps: true,
});

ParsedEventsSchema.index({ operation: 1, batch_number: 1 });

const ParsedEvents = mongoose.model('ParsedEvents', ParsedEventsSchema);

export default ParsedEvents;

