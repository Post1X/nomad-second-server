import express from 'express';
import ParsingController from '../controllers/ParsingController';
import authApiKey from '../middlewares/authApiKey';

const router = express.Router();

router.use(authApiKey);

router.post('/create', ParsingController.create);
router.get('/results/:operationId', ParsingController.getResults);
router.get('/operations', ParsingController.getOperations);
router.post('/cleanup', ParsingController.cleanup);
router.post('/sync-cities-countries', ParsingController.syncCitiesAndCountries);

export default router;

