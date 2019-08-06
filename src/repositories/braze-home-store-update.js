/* eslint-disable no-plusplus */
const axios = require('axios');
const logger = require('../config/logger.js');
const os = require('os-utils');
const iBrazeBatchSize = parseInt(process.env.BRAZE_BATCH_SIZE, 10);

const upsertUserBatch = async (customers, iBatch, iBrazeThread) => {
  logger.log('debug', `(Repository/braze-emergency-caf.upsertUserBatch) # inserting into braze BATCH ${iBatch} THREAD ${iBrazeThread}...${JSON.stringify(customers)} `);

  const payLoad = {
    api_key: process.env.BRAZE_API_KEY,
    attributes: customers
  };
  const config = {
    responseType: 'json'
  };
  const url = `${process.env.BRAZE_URL}/users/track`;

  const responseFromBRAZE = await axios.post(url, payLoad, config)
    .catch(error => {
      const {
        response = null
      } = error;
      const {
        data = null
      } = response;
      logger.log('error', `(Repository/braze-emergency-caf.upsertUserBatch) # error inserting into braze ...BATCH ${iBatch} THREAD ${iBrazeThread}...${JSON.stringify(data)} ########### ${JSON.stringify(customers.map(doc => doc.external_id))}`);
    });
  const {
    data
  } = responseFromBRAZE || null;
  logger.log('info', `(Repository/braze-emergency-caf.upsertUserBatch) # inserted into braze BATCH ${iBatch} THREAD ${iBrazeThread}...### ${JSON.stringify(data)} `);
  return data;
};
const updateHomeStore = (documents, iBatch) => {
  logger.log('info', `(Repository/braze-emergency-caf.updateCustomerEmergencyCAF) # loadToBraze entered with  batch ${iBatch}...${documents.length} `);
  const transactions = [];
  for (let iBrazeThread = 0; iBrazeThread < Math.ceil(documents.length / iBrazeBatchSize); iBrazeThread++) {
    const brazePayLoad = documents.slice(iBrazeThread * iBrazeBatchSize,
      iBrazeThread * iBrazeBatchSize + iBrazeBatchSize);
    transactions.push(upsertUserBatch(brazePayLoad, iBatch, iBrazeThread));
  }
  //var memTotal = (process.memoryUsage().heapTotal / (1024 * 1024)).toFixed(3);
  //var memUsed = (process.memoryUsage().heapUsed / (1024 * 1024)).toFixed(3);
  //var memUsedPercent = (memUsed / memTotal * 100).toFixed(3);
  //var memAvailablePercent = 100 - memUsedPercent;
  //logger.log('info', `(Repositories/braze-home-store-update.js :: Memory logging) Used/Total: ${memUsed}/${memTotal} (MB) -- Memory used percentage: ${memUsedPercent}  BATCH ${iBatch}`);
  return transactions;
};

module.exports = {
  updateHomeStore
};
