import BASE_CATEGORY_KEYWORDS from './base';

export default {
  threshold: 3,
  keywords: {
    ...BASE_CATEGORY_KEYWORDS,
    Музыка: [
      ...BASE_CATEGORY_KEYWORDS.Музыка,
      { word: 'הופעה', value: 3 },
      { word: 'קונצרט', value: 3 },
      { word: 'מוזיקה חיה', value: 3 },
      { word: 'זמר', value: 2 },
      { word: 'זמרת', value: 2 },
      { word: 'להקה', value: 2 },
      { word: 'ג\'אז', value: 2 },
      { word: 'רוק', value: 1 },
    ],
    Театр: [
      ...BASE_CATEGORY_KEYWORDS.Театр,
      { word: 'תיאטרון', value: 3 },
      { word: 'הצגה', value: 3 },
      { word: 'מחזה', value: 2 },
      { word: 'דרמה', value: 1 },
    ],
    'Шоу/Мюзиклы': [
      ...BASE_CATEGORY_KEYWORDS['Шоу/Мюзиклы'],
      { word: 'מחזמר', value: 3 },
      { word: 'מופע', value: 2 },
      { word: 'קרקס', value: 2 },
    ],
    Юмор: [
      ...BASE_CATEGORY_KEYWORDS.Юмор,
      { word: 'סטנדאפ', value: 3 },
      { word: 'סטנד-אפ', value: 3 },
      { word: 'קומדיה', value: 2 },
      { word: 'הומור', value: 2 },
    ],
    Фестивали: [
      ...BASE_CATEGORY_KEYWORDS.Фестивали,
      { word: 'פסטיבל', value: 3 },
      { word: 'פסטיבל מוזיקה', value: 3 },
    ],
    Семейное: [
      ...BASE_CATEGORY_KEYWORDS.Семейное,
      { word: 'למשפחה', value: 3 },
      { word: 'לילדים', value: 2 },
      { word: 'ילדים', value: 1 },
    ],
    Спорт: [
      ...BASE_CATEGORY_KEYWORDS.Спорт,
      { word: 'ספורט', value: 2 },
      { word: 'כדורגל', value: 2 },
      { word: 'כדורסל', value: 2 },
      { word: 'משחק', value: 1 },
    ],
    Выставки: [
      ...BASE_CATEGORY_KEYWORDS.Выставки,
      { word: 'תערוכה', value: 3 },
      { word: 'מוזיאון', value: 2 },
    ],
    Танцы: [
      ...BASE_CATEGORY_KEYWORDS.Танцы,
      { word: 'מחול', value: 2 },
      { word: 'בלט', value: 3 },
      { word: 'ריקוד', value: 2 },
    ],
    'Лекции/Семинары': [
      ...BASE_CATEGORY_KEYWORDS['Лекции/Семинары'],
      { word: 'הרצאה', value: 3 },
      { word: 'סדנה', value: 2 },
      { word: 'סמינר', value: 2 },
    ],
    Искусство: [
      ...BASE_CATEGORY_KEYWORDS.Искусство,
      { word: 'אמנות', value: 2 },
      { word: 'גלריה', value: 2 },
    ],
    Духовное: [
      ...BASE_CATEGORY_KEYWORDS.Духовное,
      { word: 'מדיטציה', value: 2 },
      { word: 'יוגה', value: 2 },
      { word: 'רוחני', value: 2 },
    ],
  },
};
