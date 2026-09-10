import mongoose from 'mongoose';

const Countries = new mongoose.Schema(
  {
    name: {
      type: String,
    },
    keywords: {
      type: [String],
      default: [],
    },
    flag_url: {
      type: String,
    },
  },
  { timestamps: true },
);

const CountriesSchema = mongoose.model('Countries', Countries);

export default CountriesSchema;
