import createError from 'http-errors';
import express from 'express';
import path from 'path';
import cookieParser from 'cookie-parser';
import logger from 'morgan';
import cors from 'cors';
import indexRouter from './routes';
import mongooseConnect from './helpers/mongooseConnect';
import { MAX_FIELDS_SIZE_MB } from './helpers/constants';

const app = express();

app.use(cors({
  origin: '*',
}));

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');

app.use(logger('dev'));
app.use(express.json({ limit: `${MAX_FIELDS_SIZE_MB}mb` }));
app.use(express.urlencoded({ extended: true, limit: `${MAX_FIELDS_SIZE_MB}mb` }));
app.use(cookieParser());

mongooseConnect(() => {
  console.log('Server initialized');
});

app.use('/', indexRouter);

// catch 404 and forward to error handler
app.use((req, res, next) => {
  next(createError(404));
});

// error handler
app.use((err, req, res, next) => {
  res.locals.message = err.message;
  res.locals.error = req.app.get('env') === 'development' ? err : {};

  res.status(err.status || 500).send({
    status: 'error',
    message: err.message,
    errors: err.errors,
  });
});

module.exports = app;

