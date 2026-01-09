import mongoose, { Schema } from 'mongoose';
import { EVENT_SOURCE } from '../helpers/constants';

const Events = new mongoose.Schema({
  name: {
    type: String,
    required: true,
  },
  name_for_search: {
    type: String,
    required: true,
    select: false,
  },
  source: {
    type: String,
    enum: Object.values(EVENT_SOURCE),
  },
  user: {
    type: Schema.Types.ObjectId,
    required: true,
    ref: 'Users',
  },
  creator_admin: {
    type: Schema.Types.ObjectId,
    required: false,
    ref: 'Admins',
  },
  operation: {
    type: Schema.Types.ObjectId,
    required: false,
    ref: 'Operations',
  },
  specialization: {
    type: String,
    required: false,
  },
  holding_date: {
    type: String,
    required: true,
  },
  date_start: {
    type: Date,
    required: false,
  },
  date_end: {
    type: Date,
    required: false,
  },
  description: {
    type: String,
    required: true,
  },
  country: {
    type: Schema.Types.ObjectId,
    required: true,
    ref: 'Countries',
  },
  city: {
    type: Schema.Types.ObjectId,
    required: true,
    ref: 'Cities',
  },
  address: {
    type: String,
  },
  coordinates: {
    lat: {
      type: String,
      required: false,
    },
    lon: {
      type: String,
      required: false,
    },
    is_special_point_on_map: {
      type: Boolean,
      default: false,
    },
  },
  contacts: {
    type: Schema.Types.ObjectId,
    required: true,
    ref: 'Contacts',
  },
  carousel_photos: {
    type: [Schema.Types.ObjectId],
    required: true,
    ref: 'Photos',
  },
  min_price: {
    type: Number,
    required: false,
    default: null,
  },
  max_price: {
    type: Number,
    required: false,
    default: null,
  },
  is_active: {
    type: Boolean,
    required: true,
    default: true,
  },
  date_of_deactivating: {
    type: Date,
    required: false,
  },
  events_category: {
    type: Schema.Types.ObjectId,
    required: true,
    ref: 'EventsCategories',
  },
}, {
  timestamps: true,
});

const EventsSchema = mongoose.model('Events', Events);

export default EventsSchema;

