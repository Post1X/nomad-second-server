import mongoose from 'mongoose';

const EventsCategoriesSchema = new mongoose.Schema({
  name: {
    type: String,
    required: true,
  },
  sort: {
    type: Number,
    default: 999,
  },
}, {
  timestamps: true,
});

const EventsCategories = mongoose.model('EventsCategories', EventsCategoriesSchema);

export default EventsCategories;
