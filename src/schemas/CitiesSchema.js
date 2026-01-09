import mongoose, { Schema } from 'mongoose';

// Упрощенная схема для городов (используется только для поиска)
// Основные данные городов приходят в meta.cities
const CitiesSchema = new mongoose.Schema({
  name: {
    type: String,
    required: true,
  },
  country_id: {
    type: Schema.Types.ObjectId,
    ref: 'Countries',
  },
  coordinates: {
    type: Schema.Types.Mixed,
  },
}, {
  timestamps: true,
});

const Cities = mongoose.model('Cities', CitiesSchema);

export default Cities;

