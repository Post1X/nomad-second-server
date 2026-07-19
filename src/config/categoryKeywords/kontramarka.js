import BASE_CATEGORY_KEYWORDS from './base';

export default {
  threshold: 3,
  keywords: {
    ...BASE_CATEGORY_KEYWORDS,
    Музыка: [
      ...BASE_CATEGORY_KEYWORDS.Музыка,
      { word: 'концертний', value: 2 },
      { word: 'живий виступ', value: 3 },
      { word: 'співак', value: 1 },
      { word: 'співачка', value: 1 },
      { word: 'гурт', value: 1 },
    ],
    Театр: [
      ...BASE_CATEGORY_KEYWORDS.Театр,
      { word: 'вистава', value: 3 },
      { word: 'театральна', value: 2 },
      { word: 'драматичний', value: 2 },
      { word: 'tour', value: 1 },
    ],
    'Шоу/Мюзиклы': [
      ...BASE_CATEGORY_KEYWORDS['Шоу/Мюзиклы'],
      { word: 'мюзикл', value: 3 },
      { word: 'шоу-програма', value: 2 },
      { word: 'цирк', value: 2 },
    ],
    Юмор: [
      ...BASE_CATEGORY_KEYWORDS.Юмор,
      { word: 'гумористичний', value: 2 },
      { word: 'стендап', value: 3 },
      { word: 'КВН', value: 2 },
    ],
    Фестивали: [
      ...BASE_CATEGORY_KEYWORDS.Фестивали,
      { word: 'фестиваль', value: 2 },
      { word: 'фест', value: 1 },
      { word: 'open air', value: 2 },
    ],
    Семейное: [
      ...BASE_CATEGORY_KEYWORDS.Семейное,
      { word: 'для дітей', value: 2 },
      { word: 'дитячий', value: 2 },
      { word: 'сімейний', value: 2 },
    ],
    Спорт: [
      ...BASE_CATEGORY_KEYWORDS.Спорт,
      { word: 'матч', value: 2 },
      { word: 'чемпіонат', value: 2 },
      { word: 'футбол', value: 2 },
    ],
    Выставки: [
      ...BASE_CATEGORY_KEYWORDS.Выставки,
      { word: 'виставка', value: 2 },
      { word: 'експозиція', value: 2 },
      { word: 'музей', value: 2 },
    ],
    Танцы: [
      ...BASE_CATEGORY_KEYWORDS.Танцы,
      { word: 'танці', value: 2 },
      { word: 'балет', value: 3 },
    ],
    'Лекции/Семинары': [
      ...BASE_CATEGORY_KEYWORDS['Лекции/Семинары'],
      { word: 'лекція', value: 2 },
      { word: 'семінар', value: 2 },
      { word: 'майстер-клас', value: 3 },
    ],
  },
};
