import mongoose from 'mongoose';

const Cities = new mongoose.Schema(
  {
    country_id: {
      type: mongoose.Schema.Types.ObjectId,
      ref: 'Countries',
    },
    name: {
      type: String,
      required: true,
    },
    sort: {
      type: Number,
      default: 999,
    },
    coordinates: {
      lat: {
        type: String,
        required: true,
      },
      lon: {
        type: String,
        required: true,
      },
    },
  },
  { timestamps: true },
);

const CitiesSchema = mongoose.model('Cities', Cities);

export default CitiesSchema;

