export const OPERATION_TYPES = {
  parsingEventsFromKontramarka: 'parsingEventsFromKontramarka',
  parsingEventsFromFienta: 'parsingEventsFromFienta',
  parsingEventsFromEventim: 'parsingEventsFromEventim',
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
};

export const ENV = process.env;

export const MAX_FIELDS_SIZE_MB = 100;
