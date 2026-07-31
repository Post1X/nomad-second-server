export const OPERATION_TYPES = {
  parsingEventsFromKontramarka: 'parsingEventsFromKontramarka',
  parsingEventsFromFienta: 'parsingEventsFromFienta',
  parsingEventsFromEventim: 'parsingEventsFromEventim',
  parsingEventsFromTicketmaster: 'parsingEventsFromTicketmaster',
  parsingEventsFromIsraelinfo: 'parsingEventsFromIsraelinfo',
};

export const OPERATION_STATUSES = {
  success: 'success',
  error: 'error',
  pending: 'pending',
  processing: 'processing',
};

export const EVENT_SOURCE = {
  nomad: 'nomad',
  kontramarka: 'kontramarka',
  fienta: 'fienta',
  eventim: 'eventim',
  ticketmaster: 'ticketmaster',
  israelinfo: 'israelinfo',
};

export const EXPIRED_EVENTS_CLEANUP_MONTHS = 6;
/** Daily cleanup: delete ParsedEvents whose date_end is older than this many days. */
export const EXPIRED_EVENTS_CLEANUP_DAYS = 2;

export const AI_CATEGORY_BATCH_CHARS = 12000;

export const SETTINGS_KEYS = {
  aiCategoryPrompt: 'ai_category_prompt',
  categoriesHash: 'categories_hash',
  lastTicketmasterParseAt: 'last_ticketmaster_parse_at',
  lastTicketmasterPullAt: 'last_ticketmaster_pull_at',
};

export const SOURCE_BY_OPERATION_TYPE = {
  [OPERATION_TYPES.parsingEventsFromKontramarka]: EVENT_SOURCE.kontramarka,
  [OPERATION_TYPES.parsingEventsFromFienta]: EVENT_SOURCE.fienta,
  [OPERATION_TYPES.parsingEventsFromEventim]: EVENT_SOURCE.eventim,
  [OPERATION_TYPES.parsingEventsFromTicketmaster]: EVENT_SOURCE.ticketmaster,
  [OPERATION_TYPES.parsingEventsFromIsraelinfo]: EVENT_SOURCE.israelinfo,
};

export const OPERATION_TYPE_BY_SOURCE = Object.fromEntries(
  Object.entries(SOURCE_BY_OPERATION_TYPE).map(([type, source]) => [source, type]),
);

export const TICKETMASTER_PARSE_INTERVAL_DAYS = 21;

/** ISO 3166-1 alpha-2 — страны с покрытием Ticketmaster Discovery (Discovery Feed docs). */
export const TICKETMASTER_COUNTRY_CODES = [
  'AE', 'AT', 'AU', 'BE', 'BR', 'CA', 'CH', 'CL', 'CZ', 'DE', 'DK', 'ES',
  'FI', 'FR', 'GB', 'IE', 'IT', 'KE', 'MX', 'NL', 'NO', 'NZ', 'PE', 'PL',
  'SE', 'TR', 'UG', 'US', 'ZA',
];

export const ENV = process.env;

export const MAX_FIELDS_SIZE_MB = 100;
