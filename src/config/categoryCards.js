/**
 * Rich category cards for AI prompts (option A).
 * Key = exact EventsCategories.name (RU).
 * Bump CATEGORY_CARDS_VERSION when editing cards so prompt cache rebuilds.
 */

export const CATEGORY_CARDS_VERSION = 'cards-v4-web-genre';

/**
 * @typedef {{ use: string, examples: string[], not: string }} CategoryCard
 */

/** @type {Record<string, CategoryCard>} */
const CARDS = {
  Музыка: {
    use: 'Live music as the main content: bands, singers, DJ sets, orchestras, opera, song concerts.',
    examples: [
      'Imagine Dragons — Golden Circle Upgrade',
      'Jazz Night / симфонический оркестр',
      'QUEST PISTOLS — LIVE',
    ],
    not: 'Comedy/humor shows even if titled «концерт»; stand-up; theatre plays; film screenings; lectures.',
  },
  Театр: {
    use: 'Stage drama / play / theatrical performance (спектакли, драма).',
    examples: ['Гамлет', 'Contemporary drama premiere', 'Мой внук Вениамин'],
    not: 'Musicals/circus (→ Шоу/Мюзиклы), pure concerts, cinema, stand-up.',
  },
  'Шоу/Мюзиклы': {
    use: 'Musicals, circus, variety/spectacle shows, ice shows, large production entertainment.',
    examples: ['The Nutcracker — Premium Comfort', 'Cirque show'],
    not: 'Straight drama theatre, club DJ nights, cinema, stand-up comedy.',
  },
  Юмор: {
    use: 'Stand-up, comedy, satire, humorous one-man shows, parody/humor magazines on stage, '
      + 'comedians even when the listing says «концерт» (e.g. Galkin, Slepakov, Красная Бурда).',
    examples: [
      'Stand-up evening',
      'МАКСИМ ГАЛКИН — Юбилейные концерты',
      'КРАСНАЯ БУРДА',
      'СЕМЕН СЛЕПАКОВ',
    ],
    not: 'Serious music concerts by singers/bands, theatre drama, dry lectures.',
  },
  Фестивали: {
    use: 'Multi-day / multi-artist festivals as the main format (city fest, open-air fest).',
    examples: ['City Summer Festival', 'Street Food Fest (as festival)'],
    not: 'Single concert billed as a tour date (→ Музыка), single exhibition.',
  },
  Семейное: {
    use: 'Kids / family-oriented events, children shows, family theatre, school holiday shows.',
    examples: ['Детский спектакль', 'Ёлки-шоу', "Children's workshop"],
    not: 'Adult nightlife, 18+ stand-up, regular adult concerts without kids focus.',
  },
  Искусство: {
    use: 'Art-focused events: galleries openings, art performances, art talks not full lectures track.',
    examples: ['Art gallery opening', 'Contemporary art night'],
    not: 'Museum exhibitions as exhibition format (→ Выставки), cinema.',
  },
  Духовное: {
    use: 'Religious / spiritual gatherings, liturgy-related public events.',
    examples: ['Концерт духовной музыки в храме', 'Spiritual retreat talk'],
    not: 'Secular concerts, general lectures.',
  },
  Спорт: {
    use: 'Sports matches, tournaments, fitness competitions, races.',
    examples: ['Football match', 'Marathon / турнир'],
    not: 'Esports if no sports category fit ambiguity — prefer null for discovery; dance fitness shows → Танцы if dance.',
  },
  Выставки: {
    use: 'Exhibitions, museum expositions, trade/expo shows.',
    examples: ['Выставка в музее', 'Photo exhibition'],
    not: 'Single art performance night without exposition (→ Искусство), cinema.',
  },
  Танцы: {
    use: 'Dance performances, ballet, dance battles, dance shows.',
    examples: ['Балет', 'Dance battle'],
    not: 'Club DJ party without dance performance focus (→ Музыка), musicals → Шоу/Мюзиклы.',
  },
  'Лекции/Семинары': {
    use: 'Talks, lectures, seminars, educational workshops, masterclasses (talk-like), '
      + 'author meetings, literary evenings / литературный концерт, psychologist talks.',
    examples: [
      'Лекция',
      'Встреча с писателем / журналистом',
      'Большой литературный концерт Дины Рубиной',
      'Михаил Лабковский',
    ],
    not: 'Music band concerts, theatre plays, stand-up comedy as pure humor.',
  },
  Кино: {
    use: 'Film screenings, cinema premieres, documentary film shows.',
    examples: ['Кинопремьера', 'Документальный фильм показ'],
    not: 'Live concerts, theatre plays, fashion runway shows, comedy shows titled «показ».',
  },
  Мода: {
    use: 'Fashion shows, runway, designer collections presentations.',
    examples: ['Fashion week runway', 'Показ коллекции дизайнера'],
    not: 'Film «показ» (screening → Кино), comedy shows, concerts.',
  },
  Детям: {
    use: 'Events explicitly for small children when not better as Семейное.',
    examples: ['Музыкальная сказка для детей 2–6'],
    not: 'Adult concerts, 18+ comedy.',
  },
};

/**
 * @param {string} name
 * @returns {CategoryCard|null}
 */
export const getCategoryCard = (name) => {
  if (!name || name === 'Другое') return null;
  return CARDS[name] || null;
};

/**
 * Format cards block for AI system prompt.
 * @param {Array<{ _id: any, name: string }>} categories
 */
export const formatCategoryCardsForPrompt = (categories) => {
  const lines = [];
  for (const c of categories || []) {
    if (!c?.name || c.name === 'Другое') continue;
    const card = getCategoryCard(c.name);
    const id = String(c._id);
    if (!card) {
      lines.push(`- ${id} | ${c.name}: (no card — use name meaning carefully)`);
      continue;
    }
    lines.push(
      `- ${id} | ${c.name}\n`
      + `  use: ${card.use}\n`
      + `  examples: ${card.examples.join(' · ')}\n`
      + `  NOT: ${card.not}`,
    );
  }
  return lines.join('\n');
};

export default CARDS;
