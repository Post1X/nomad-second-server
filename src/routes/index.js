import express from 'express';
import parsingRouter from './parsing';

const router = express.Router();

/* GET home page. */
router.get('/', (req, res) => {
  res.json({
    status: 'ok',
    message: 'Nomad Second Server - Parsing Service',
  });
});

router.get('/ping', (req, res) => res.json('pong'));

router.use('/parsing', parsingRouter);

export default router;

