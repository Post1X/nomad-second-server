import BASE_CATEGORY_KEYWORDS from './base';

export default {
  threshold: 2,
  keywords: {
    ...BASE_CATEGORY_KEYWORDS,
    'Фестивали': [
      ...BASE_CATEGORY_KEYWORDS['Фестивали'],
      { word: 'fienta', value: 1 },
    ],
  },
};
