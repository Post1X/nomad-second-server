import BASE_CATEGORY_KEYWORDS from './base';

export default {
  threshold: 3,
  keywords: {
    ...BASE_CATEGORY_KEYWORDS,
    Музыка: [
      ...BASE_CATEGORY_KEYWORDS.Музыка,
      { word: 'Livekonzert', value: 3 },
      { word: 'Rockkonzert', value: 3 },
      { word: 'Popkonzert', value: 3 },
      { word: 'Klassikkonzert', value: 3 },
      { word: 'konzert', value: 2 },
      { word: 'Sänger', value: 2 },
      { word: 'Sängerin', value: 2 },
      { word: 'Band', value: 1 },
      { word: 'live', value: 1 },
    ],
    Театр: [
      ...BASE_CATEGORY_KEYWORDS.Театр,
      { word: 'Schauspielhaus', value: 3 },
      { word: 'Bühnenstück', value: 3 },
      { word: 'Theaterstück', value: 3 },
      { word: 'Aufführung', value: 2 },
      { word: 'Drama', value: 1 },
    ],
    'Шоу/Мюзиклы': [
      ...BASE_CATEGORY_KEYWORDS['Шоу/Мюзиклы'],
      { word: 'Musicalshow', value: 3 },
      { word: 'Varieté', value: 2 },
      { word: 'Zirkus', value: 2 },
    ],
    Юмор: [
      ...BASE_CATEGORY_KEYWORDS.Юмор,
      { word: 'Comedy-Show', value: 3 },
      { word: 'Stand-up-Comedy', value: 3 },
      { word: 'Komiker', value: 2 },
      { word: 'Kabarettabend', value: 3 },
    ],
    Фестивали: [
      ...BASE_CATEGORY_KEYWORDS.Фестивали,
      { word: 'Musikfestival', value: 3 },
      { word: 'Sommerfestival', value: 2 },
      { word: 'Festivals', value: 2 },
    ],
    Семейное: [
      ...BASE_CATEGORY_KEYWORDS.Семейное,
      { word: 'Familienkonzert', value: 3 },
      { word: 'Kindertheater', value: 3 },
      { word: 'für Kinder', value: 2 },
    ],
    Спорт: [
      ...BASE_CATEGORY_KEYWORDS.Спорт,
      { word: 'Fußball', value: 2 },
      { word: 'Bundesliga', value: 3 },
      { word: 'Handball', value: 2 },
      { word: 'Eishockey', value: 2 },
    ],
    Выставки: [
      ...BASE_CATEGORY_KEYWORDS.Выставки,
      { word: 'Messe', value: 2 },
      { word: 'Kunstausstellung', value: 3 },
    ],
    Танцы: [
      ...BASE_CATEGORY_KEYWORDS.Танцы,
      { word: 'Ballett', value: 3 },
      { word: 'Tanzshow', value: 3 },
    ],
    'Лекции/Семинары': [
      ...BASE_CATEGORY_KEYWORDS['Лекции/Семинары'],
      { word: 'Vortrag', value: 2 },
      { word: 'Lesung', value: 2 },
      { word: 'Workshop', value: 2 },
    ],
  },
};
