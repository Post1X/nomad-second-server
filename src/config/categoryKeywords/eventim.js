import BASE_CATEGORY_KEYWORDS from './base';

export default {
  threshold: 2,
  keywords: {
    ...BASE_CATEGORY_KEYWORDS,
    'Музыка': [
      ...BASE_CATEGORY_KEYWORDS['Музыка'],
      { word: 'konzert', value: 1 },
      { word: 'live', value: 1 },
    ],
  },
};
