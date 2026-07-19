import BASE_CATEGORY_KEYWORDS from './base';

export default {
  threshold: 3,
  keywords: {
    ...BASE_CATEGORY_KEYWORDS,
    Музыка: [
      ...BASE_CATEGORY_KEYWORDS.Музыка,
      { word: 'kontsert', value: 2 },
      { word: 'muusika', value: 2 },
      { word: 'live-muusika', value: 3 },
      { word: 'orkester', value: 2 },
      { word: 'laulja', value: 1 },
    ],
    Театр: [
      ...BASE_CATEGORY_KEYWORDS.Театр,
      { word: 'teater', value: 2 },
      { word: 'etendus', value: 2 },
      { word: 'lavastus', value: 2 },
      { word: 'näidend', value: 2 },
    ],
    'Шоу/Мюзиклы': [
      ...BASE_CATEGORY_KEYWORDS['Шоу/Мюзиклы'],
      { word: 'muusikal', value: 3 },
      { word: 'show', value: 1 },
    ],
    Юмор: [
      ...BASE_CATEGORY_KEYWORDS.Юмор,
      { word: 'standup', value: 2 },
      { word: 'komöödia', value: 2 },
      { word: 'huumor', value: 2 },
    ],
    Фестивали: [
      ...BASE_CATEGORY_KEYWORDS.Фестивали,
      { word: 'festival', value: 2 },
      { word: 'festivál', value: 2 },
      { word: 'vabaõhu', value: 2 },
    ],
    Семейное: [
      ...BASE_CATEGORY_KEYWORDS.Семейное,
      { word: 'perele', value: 2 },
      { word: 'lastele', value: 2 },
      { word: 'lasteüritus', value: 3 },
    ],
    Спорт: [
      ...BASE_CATEGORY_KEYWORDS.Спорт,
      { word: 'spordiüritus', value: 3 },
      { word: 'jalgpall', value: 2 },
      { word: 'maraton', value: 2 },
    ],
    Выставки: [
      ...BASE_CATEGORY_KEYWORDS.Выставки,
      { word: 'näitus', value: 2 },
      { word: 'muuseum', value: 2 },
    ],
    Танцы: [
      ...BASE_CATEGORY_KEYWORDS.Танцы,
      { word: 'tants', value: 2 },
      { word: 'ballett', value: 3 },
    ],
    'Лекции/Семинары': [
      ...BASE_CATEGORY_KEYWORDS['Лекции/Семинары'],
      { word: 'loeng', value: 2 },
      { word: 'seminar', value: 2 },
      { word: 'töötuba', value: 2 },
    ],
  },
};
