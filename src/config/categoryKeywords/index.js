import { EVENT_SOURCE } from '../../helpers/constants';
import eventim from './eventim';
import fienta from './fienta';
import kontramarka from './kontramarka';
import ticketmaster from './ticketmaster';
import israelinfo from './israelinfo';

const BY_SOURCE = {
  [EVENT_SOURCE.eventim]: eventim,
  [EVENT_SOURCE.fienta]: fienta,
  [EVENT_SOURCE.kontramarka]: kontramarka,
  [EVENT_SOURCE.ticketmaster]: ticketmaster,
  [EVENT_SOURCE.israelinfo]: israelinfo,
};

export const getCategoryConfigForSource = (source) => {
  return BY_SOURCE[source] || eventim;
};

export default getCategoryConfigForSource;
