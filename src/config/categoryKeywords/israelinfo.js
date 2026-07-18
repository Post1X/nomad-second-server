import BASE_CATEGORY_KEYWORDS from './base';

export default {
  threshold: 2,
  keywords: {
    ...BASE_CATEGORY_KEYWORDS,
    'Музыка': [
      ...BASE_CATEGORY_KEYWORDS['Музыка'],
      { word: 'הופעה', value: 1 },
      { word: 'קונצרט', value: 1 },
    ],
    'Театр': [
      ...BASE_CATEGORY_KEYWORDS['Театр'],
      { word: 'תיאטרון', value: 1 },
      { word: 'הצגה', value: 1 },
    ],
    'Спорт': [
      ...BASE_CATEGORY_KEYWORDS['Спорт'],
      { word: 'ספורט', value: 1 },
    ],
  },
};
