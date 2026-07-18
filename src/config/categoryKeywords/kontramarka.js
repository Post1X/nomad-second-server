import BASE_CATEGORY_KEYWORDS from './base';

export default {
  threshold: 2,
  keywords: {
    ...BASE_CATEGORY_KEYWORDS,
    'Театр': [
      ...BASE_CATEGORY_KEYWORDS['Театр'],
      { word: 'kontramarka', value: 1 },
      { word: 'tour', value: 1 },
    ],
  },
};
