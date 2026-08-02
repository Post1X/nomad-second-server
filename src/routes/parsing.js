import express from 'express';
import ParsingController from '../controllers/ParsingController';
import authApiKey from '../middlewares/authApiKey';

const router = express.Router();

router.get('/stats-ui', ParsingController.statsPage);
router.get('/events-ui', ParsingController.eventsPage);
router.get('/cities-ui', ParsingController.citiesPage);

router.use(authApiKey);

router.post('/create', ParsingController.create);
router.get('/events/browse', ParsingController.browseEvents);
router.get('/events', ParsingController.getEvents);
router.post('/events/ack', ParsingController.ackEvents);
router.get('/runs', ParsingController.getRuns);
router.get('/results/:runId', ParsingController.getResults);
router.post('/runs/:runId/stop', ParsingController.stopParseRun);
router.get('/cron', ParsingController.getCron);
router.post('/cron/stop', ParsingController.stopCron);
router.post('/cron/start', ParsingController.startCron);
router.post('/cron/:jobId/enable', (req, res, next) => {
  req.body = { ...(req.body || {}), enabled: true };
  return ParsingController.setCronJob(req, res, next);
});
router.post('/cron/:jobId/disable', (req, res, next) => {
  req.body = { ...(req.body || {}), enabled: false };
  return ParsingController.setCronJob(req, res, next);
});
router.post('/cron/:jobId/run', ParsingController.runCronJobNow);
router.get('/stats/weekly', ParsingController.getWeeklyStats);
router.get('/stats/backfill', ParsingController.getBackfillStats);
router.post('/cleanup', ParsingController.cleanup);
router.post('/categorize-batch', ParsingController.categorizeBatch);
router.post('/events/lookup', ParsingController.lookupEvents);
router.post('/events/enrich-ticketmaster', ParsingController.enrichTicketmaster);
router.post('/sync-cities-countries', ParsingController.syncCitiesAndCountries);
router.post('/submit-fienta-html', ParsingController.submitFientaHtml);

router.get('/countries', ParsingController.getCountries);
router.post('/cities/discover', ParsingController.discoverCities);
router.get('/cities/suggestions', ParsingController.listCitySuggestions);
router.get('/cities/suggestions/metrics', ParsingController.citySuggestionsMetrics);
router.post('/cities/suggestions/:id/approve', ParsingController.approveCitySuggestion);
router.post('/cities/suggestions/:id/reject', ParsingController.rejectCitySuggestion);

router.get('/categories/suggestions', ParsingController.listCategorySuggestions);
router.get('/categories/suggestions/metrics', ParsingController.categorySuggestionsMetrics);
router.post('/categories/suggestions/backfill', ParsingController.startCategorySuggestionsBackfill);
router.get('/categories/suggestions/backfill', ParsingController.getCategorySuggestionsBackfill);
router.post('/categories/suggestions/backfill/stop', ParsingController.stopCategorySuggestionsBackfill);
router.post('/categories/suggestions/consolidate', ParsingController.startCategorySuggestionsConsolidate);
router.get('/categories/suggestions/consolidate', ParsingController.getCategorySuggestionsConsolidate);
router.post('/categories/suggestions/consolidate/stop', ParsingController.stopCategorySuggestionsConsolidate);
router.post('/categories/suggestions/:id/approve', ParsingController.approveCategorySuggestion);
router.post('/categories/suggestions/:id/reject', ParsingController.rejectCategorySuggestion);

export default router;
