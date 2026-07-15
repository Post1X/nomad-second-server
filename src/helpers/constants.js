export const OPERATION_TYPES = {
  parsingEventsFromKontramarka: 'parsingEventsFromKontramarka',
  parsingEventsFromFienta: 'parsingEventsFromFienta',
  parsingEventsFromEventim: 'parsingEventsFromEventim',
  parsingEventsFromTicketmaster: 'parsingEventsFromTicketmaster',
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
};

/** ISO 3166-1 alpha-2 — страны с покрытием Ticketmaster Discovery (Discovery Feed docs). */
export const TICKETMASTER_COUNTRY_CODES = [
  'AE', 'AT', 'AU', 'BE', 'BR', 'CA', 'CH', 'CL', 'CZ', 'DE', 'DK', 'ES',
  'FI', 'FR', 'GB', 'IE', 'IT', 'KE', 'MX', 'NL', 'NO', 'NZ', 'PE', 'PL',
  'SE', 'TR', 'UG', 'US', 'ZA',
];

export const ENV = process.env;

export const MAX_FIELDS_SIZE_MB = 100;
