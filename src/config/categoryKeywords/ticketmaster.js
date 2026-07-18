import BASE_CATEGORY_KEYWORDS from './base';

export default {
  threshold: 3,
  keywords: {
    ...BASE_CATEGORY_KEYWORDS,
    'Музыка': [
      ...BASE_CATEGORY_KEYWORDS['Музыка'],
      { word: 'Music', value: 1 },
      { word: 'Rock', value: 1 },
      { word: 'Pop', value: 1 },
      { word: 'Classical', value: 1 },
    ],
    'Спорт': [
      ...BASE_CATEGORY_KEYWORDS['Спорт'],
      { word: 'Sports', value: 1 },
      { word: 'NBA', value: 1 },
      { word: 'NFL', value: 1 },
      { word: 'MLS', value: 1 },
    ],
  },
};
