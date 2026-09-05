/**
 * Крупные города Европы: ключ — русское название, значения — варианты написания.
 * Используется всеми парсерами (Ticketmaster, Eventim, Fienta, Kontramarka).
 */

const normalize = (str = '') => str
  .toString()
  .toLowerCase()
  .normalize('NFD')
  .replace(/[\u0300-\u036f]/g, '')
  .replace(/\s+/g, ' ')
  .trim();

/** @type {Record<string, string[]>} */
export const CITY_ALIASES = {
  // Польша
  'Варшава': ['Warsaw', 'Warszawa', 'Varsovie'],
  'Краков': ['Krakow', 'Kraków', 'Cracow'],
  'Вроцлав': ['Wroclaw', 'Wrocław', 'Breslau'],
  'Познань': ['Poznan', 'Poznań'],
  'Гданьск': ['Gdansk', 'Gdańsk', 'Danzig'],
  'Лодзь': ['Lodz', 'Łódź'],
  'Катовице': ['Katowice'],
  'Щецин': ['Szczecin', 'Stettin'],
  'Люблин': ['Lublin'],
  'Белосток': ['Bialystok', 'Białystok'],
  'Гдиня': ['Gdynia'],
  // DB typo variant (legacy row name)
  'Гдиниа': ['Gdynia', 'Гдиня'],
  'Сопот': ['Sopot'],
  'Гливице': ['Gliwice'],
  'Плоцк': ['Plock', 'Płock'],
  'Забже': ['Zabrze'],
  'Ржешов': ['Rzeszow', 'Rzeszów'],
  'Ольштын': ['Olsztyn'],
  'Иновроцлав': ['Inowroclaw', 'Inowrocław'],
  'Жагань': ['Zagan', 'Żagań'],
  'Казимеж-Долный': ['Kazimierz Dolny', 'Kazimierz'],
  'Тыхы': ['Tychy'],
  'Бытом': ['Bytom'],
  'Хожув': ['Chorzow', 'Chorzów'],
  'Рыбник': ['Rybnik'],
  'Тарнов': ['Tarnow', 'Tarnów'],
  'Кельце': ['Kielce'],
  'Торунь': ['Torun', 'Toruń'],
  'Быдгощ': ['Bydgoszcz'],

  // Германия
  'Берлин': ['Berlin'],
  'Мюнхен': ['Munich', 'München', 'Muenchen'],
  'Гамбург': ['Hamburg'],
  'Кёльн': ['Cologne', 'Köln', 'Koeln'],
  'Франкфурт': ['Frankfurt', 'Frankfurt am Main'],
  'Штутгарт': ['Stuttgart'],
  'Дюссельдорф': ['Dusseldorf', 'Düsseldorf'],
  'Лейпциг': ['Leipzig'],
  'Дрезден': ['Dresden'],
  'Ганновер': ['Hanover', 'Hannover'],
  'Нюрнберг': ['Nuremberg', 'Nürnberg', 'Nuernberg'],
  'Бремен': ['Bremen'],
  'Дортмунд': ['Dortmund'],
  'Эссен': ['Essen'],
  'Дуйсбург': ['Duisburg'],
  'Бохум': ['Bochum'],
  'Бонн': ['Bonn'],
  'Мюнстер': ['Munster', 'Münster'],
  'Карлсруэ': ['Karlsruhe'],
  'Мангейм': ['Mannheim'],
  'Аугсбург': ['Augsburg'],
  'Висбаден': ['Wiesbaden'],

  // Великобритания и Ирландия
  'Лондон': ['London'],
  'Манчестер': ['Manchester'],
  'Бирмингем': ['Birmingham'],
  'Ливерпуль': ['Liverpool'],
  'Глазго': ['Glasgow'],
  'Эдинбург': ['Edinburgh'],
  'Лидс': ['Leeds'],
  'Бристоль': ['Bristol'],
  'Кардифф': ['Cardiff'],
  'Белфаст': ['Belfast'],
  'Ньюкасл': ['Newcastle', 'Newcastle upon Tyne'],
  'Ноттингем': ['Nottingham'],
  'Шеффиелд': ['Sheffield'],
  'Брайтон': ['Brighton', 'Brighton and Hove'],
  'Дублин': ['Dublin'],
  'Корк': ['Cork'],

  // Франция
  'Париж': ['Paris'],
  'Лион': ['Lyon', 'Lyons'],
  'Марсель': ['Marseille', 'Marsailles'],
  'Тулуза': ['Toulouse'],
  'Ницца': ['Nice'],
  'Нант': ['Nantes'],
  'Страсбург': ['Strasbourg', 'Strassburg'],
  'Бордо': ['Bordeaux'],
  'Лиль': ['Lille'],
  'Монпелье': ['Montpellier'],
  'Ренн': ['Rennes'],
  'Реймс': ['Reims'],
  'Гренобль': ['Grenoble'],
  'Нанси': ['Nancy'],

  // Испания
  'Мадрид': ['Madrid'],
  'Барселона': ['Barcelona'],
  'Валенсия': ['Valencia', 'València'],
  'Севилья': ['Seville', 'Sevilla'],
  'Бильбао': ['Bilbao'],
  'Малага': ['Malaga', 'Málaga'],
  'Сарагоса': ['Zaragoza', 'Saragossa'],
  'Мурсия': ['Murcia'],
  'Пальма': ['Palma', 'Palma de Mallorca'],

  // Италия
  'Рим': ['Rome', 'Roma'],
  'Милан': ['Milan', 'Milano'],
  'Неаполь': ['Naples', 'Napoli'],
  'Турин': ['Turin', 'Torino'],
  'Флоренция': ['Florence', 'Firenze'],
  'Болонья': ['Bologna'],
  'Верона': ['Verona'],
  'Генуя': ['Genoa', 'Genova'],
  'Палермо': ['Palermo'],
  'Бари': ['Bari'],
  'Катания': ['Catania'],
  'Венеция': ['Venice', 'Venezia'],
  'Триест': ['Trieste'],

  // Нидерланды и Бельгия
  'Амстердам': ['Amsterdam'],
  'Роттердам': ['Rotterdam'],
  'Гаага': ['The Hague', 'Den Haag', '\'s-Gravenhage'],
  'Утрехт': ['Utrecht'],
  'Эйндховен': ['Eindhoven'],
  'Брюссель': ['Brussels', 'Bruxelles', 'Brussel'],
  'Антверпен': ['Antwerp', 'Antwerpen', 'Anvers'],
  'Гент': ['Ghent', 'Gent', 'Gand'],
  'Брюгге': ['Bruges', 'Brugge'],
  'Лиеж': ['Liege', 'Liège', 'Luik'],

  // Австрия и Швейцария
  'Вена': ['Vienna', 'Wien'],
  'Зальцбург': ['Salzburg'],
  'Грац': ['Graz'],
  'Инсбрук': ['Innsbruck'],
  'Цюрих': ['Zurich', 'Zürich'],
  'Женева': ['Geneva', 'Genève', 'Genf'],
  'Базель': ['Basel', 'Basle'],
  'Берн': ['Bern', 'Berne'],
  'Лозанна': ['Lausanne'],
  'Люцерн': ['Lucerne', 'Luzern'],

  // Чехия, Словакия, Венгрия
  'Прага': ['Prague', 'Praha'],
  'Брно': ['Brno'],
  'Острава': ['Ostrava'],
  'Братислава': ['Bratislava'],
  'Будапешт': ['Budapest'],

  // Скandinaviya
  'Стокгольм': ['Stockholm'],
  'Готенборг': ['Gothenburg', 'Göteborg'],
  'Мальмё': ['Malmo', 'Malmö'],
  'Уппсала': ['Uppsala'],
  'Осло': ['Oslo'],
  'Берген': ['Bergen'],
  'Ставангер': ['Stavanger'],
  'Копенгаген': ['Copenhagen', 'København', 'Kobenhavn'],
  'Орхус': ['Aarhus', 'Århus', 'Arhus'],
  'Оденсе': ['Odense'],
  'Хельсинки': ['Helsinki', 'Helsingfors'],
  'Тампере': ['Tampere'],
  'Турку': ['Turku', 'Abo'],
  'Рейкьявик': ['Reykjavik', 'Reykjavík'],

  // Португалия и Греция
  'Лиссабон': ['Lisbon', 'Lisboa'],
  'Порту': ['Porto', 'Oporto'],
  'Афины': ['Athens', 'Athina'],
  'Салоники': ['Thessaloniki', 'Salonika', 'Thessalonica'],

  // Румыния, Болгария, Балканы
  'Бухарест': ['Bucharest', 'Bucuresti', 'București'],
  'Клуж-Напока': ['Cluj-Napoca', 'Cluj'],
  'Тимишоара': ['Timisoara', 'Timișoara'],
  'София': ['Sofia'],
  'Пловдив': ['Plovdiv'],
  'Варна': ['Varna'],
  'Белград': ['Belgrade', 'Beograd'],
  'Нови-Сад': ['Novi Sad'],
  'Загреб': ['Zagreb'],
  'Сплит': ['Split'],
  'Любляна': ['Ljubljana'],
  'Сараево': ['Sarajevo'],
  'Скопье': ['Skopje'],
  'Тирана': ['Tirana'],

  // Балтия
  'Вильнюс': ['Vilnius', 'Wilno'],
  'Каунас': ['Kaunas'],
  'Рига': ['Riga', 'Rīga'],
  'Таллин': ['Tallinn'],

  // Украина
  'Киев': ['Kyiv', 'Kiev', 'Kiyev'],
  'Львов': ['Lviv', 'Lvov', 'Lwów'],
  'Одесса': ['Odessa', 'Odesa'],
  'Харьков': ['Kharkiv', 'Kharkov'],
  'Днепр': ['Dnipro', 'Dnipropetrovsk', 'Dnepropetrovsk'],

  // Прочие
  'Люксембург': ['Luxembourg', 'Luxemburg'],
  'Валлетта': ['Valletta'],
  'Никосия': ['Nicosia', 'Lefkosia'],
};

const aliasToRussianKey = new Map();
for (const [russianKey, aliases] of Object.entries(CITY_ALIASES)) {
  aliasToRussianKey.set(normalize(russianKey), russianKey);
  for (const alias of aliases) {
    aliasToRussianKey.set(normalize(alias), russianKey);
  }
}

/** @returns {string[]} уникальные варианты для поиска (оригинал + русское имя + алиасы) */
export const getCitySearchVariants = (cityName = '') => {
  const original = String(cityName || '').trim();
  if (!original) return [];

  const variants = new Set([original]);
  const key = normalize(original);
  variants.add(key);

  const russianKey = aliasToRussianKey.get(key);
  if (russianKey) {
    variants.add(russianKey);
    variants.add(normalize(russianKey));
    for (const alias of CITY_ALIASES[russianKey]) {
      variants.add(alias);
      variants.add(normalize(alias));
    }
  }

  return [...variants].filter(Boolean);
};

export default CITY_ALIASES;
