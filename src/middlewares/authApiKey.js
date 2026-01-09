import { ENV } from '../helpers/constants';

const PARSING_SERVER_API_KEY = ENV.PARSING_SERVER_API_KEY;

export default function authApiKey(req, res, next) {
  const apiKey = req.headers['x-api-key'];

  if (!apiKey) {
    return res.status(401).json({
      status: 'error',
      message: 'Missing X-API-Key header',
    });
  }

  if (apiKey !== PARSING_SERVER_API_KEY) {
    return res.status(403).json({
      status: 'error',
      message: 'Invalid API key',
    });
  }

  next();
}

