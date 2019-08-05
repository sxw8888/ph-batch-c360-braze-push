/* eslint-disable no-await-in-loop */
// eslint-disable-next-line no-unused-vars
const os = require('os-utils');
const config = require('../config/config.js');
const logger = require('../config/logger.js');
const options = require('../config/databaseConfig.js');
const snowflakeC360 = require('../repositories/snowflakeC360.js');
const brazeHomeStoreUpdate = require('../repositories/braze-home-store-update.js');

const iBatchSize = parseInt(process.env.BATCH_SIZE, 10);
let totalBatches_HOME_STORE_UPDATES = 1;

const fetchCounts = async (processDate, snowConnection, table_name, where) => {
  logger.info(`(Service/mergeData.fetchCounts) # fetching counts...`);
  //console.log("Fetching count..");
   const rows = await snowflakeC360.getCounts(processDate, snowConnection, table_name, where).catch(error => {
    logger.error(`(Service/mergeData.fetchCounts) # Unable to execute query on SNOWFLAKE DB : ${JSON.stringify(error)}`);
    //console.log("Error getting count");
    return Promise.reject(new Error(error));
  });

  if (rows && rows.length > 0) {
    rows.forEach(doc => {
      logger.info(`(Service/mergeData.fetchCounts) # counts...${doc.CATEGORY}, ${doc.TOTALCOUNTS}`);
      //console.log("Got count: " + doc.TOTALCOUNTS);
	if (doc.CATEGORY === 'MAB_DEMO') { totalBatches_HOME_STORE_UPDATES = Math.ceil(doc.TOTALCOUNTS / iBatchSize); }
    });
   return Promise.resolve(rows);

  }
  return Promise.reject(new Error('rows were empty'));
};

const fetchAndUpdateHomeStore = async (processDate, snowConnection, table_name, columns, where, startBatch) => {
  const statusFetchAndUpdateHomeStore = [];
  let fromBatch = 0;
  // eslint-disable-next-line no-plusplus 
  if(startBatch) {
     fromBatch = startBatch  
     logger.info(`(Service/mergeData.fetchAndUpdateHomeStore) # Starting Batch set up to: ${fromBatch}`);   
  } else {
     logger.info(`(Service/mergeData.fetchAndUpdateHomeStore) # Starting Batch set up to: ${fromBatch}`);  
  }

  for (let iBatch = fromBatch; iBatch < totalBatches_HOME_STORE_UPDATES; iBatch++) { //totalBatches_HOME_STORE_UPDATES
    logger.info(`(Service/mergeData.fetchAndUpdateHomeStore) # Processing Batch: ${iBatch}`);
    let batchUpdateHomeStoreUpdates = [];
    const transactionOperations = [];
    let customerHomeStoreUpdate = {};
    
   
    const capturedHomeStoreUpdates = await snowflakeC360.fetchHomeStoreUpdates(processDate,
      snowConnection, iBatch * iBatchSize, iBatchSize,  table_name, columns, where)
      .catch(error => {
        logger.error(` Service/mergeData.fetchAndUpdateHomeStore # Unable to execute query on SNOWFLAKE DB : ${JSON.stringify(error)}`);
        return Promise.reject(error);
      });
    
    if(capturedHomeStoreUpdates){
     
      batchUpdateHomeStoreUpdates = capturedHomeStoreUpdates.map(row => {
            //let K = row.COLUMN_1;
        
            customerHomeStoreUpdate = {
              external_id: row.EXTERNAL_ID,
              //[K]: row.VALUE_1
              email: row.EMAIL_ADDRESS,
              home_store: row.HOME_STORE,
              time_zone: row.TIME_ZONE,
              OPT_OUT_FLAG: row.OPT_OUT_FLAG,
              OFFER_A: row.OFFER_A,
              OFFER_B: row.OFFER_B,
              OFFER_C: row.OFFER_C,
              OFFER_D: row.OFFER_D,
              OFFER_E: row.OFFER_E,
              OFFER_F: row.OFFER_F,
              OFFER_G: row.OFFER_G,
	      "_update_existing_only" : true
          };
          return customerHomeStoreUpdate;
        });
    }else{
	logger.info(`Returned empty updates`);
	return Promise.reject('Returned empty updates');
	}
   
    transactionOperations.push(brazeHomeStoreUpdate.updateHomeStore(batchUpdateHomeStoreUpdates,
      iBatch));
    const updateHomeStore = await Promise.all(transactionOperations).catch(error => {
      logger.error(`(Service/mergeData.fetchAndUpdateHomeStore-updateHomeStore) # Error promiseResponse : ${error}`);
     	return Promise.reject(error);
    });

    logger.info(`(Service/mergeData.fetchAndUpdateHomeStore-updateHomeStore) # effected records Braze ...${updateHomeStore}`);
    statusFetchAndUpdateHomeStore.push(updateHomeStore);
  }
  return Promise.resolve(statusFetchAndUpdateHomeStore);
};


const connectAndProcess = async (processDate,table_name,columns,where,startBatch) => {
  const snowConnection = await snowflakeC360.snowflakeDBConnection(options.snowflakeDBOption)
    .catch(error => {
      logger.log('error', `(home-store-update) # Captured SNOWFLAKE DB Connection error from Data layer Aborting.. ${JSON.stringify(error)}`);
      return Promise.reject(error);
    });

  logger.log('info', `(Service/mergeData.connectAndProcess) # Data layer indicated SUCCESSFUL SNOWFLAKE Connection # ${snowConnection}`);
  // const mongoConnection = await mongoCustomerRecovery.mongoDBConnection(process.env.DBURL
  // , options.mongoDBOptions)
  //   .catch(error => {
  //     logger.log('error', `(Service/mergeData.connectAndProcess) #
  // Captured MongoDB Connection error from Data layer Aborting.. ${JSON.stringify(error)}`);
  //     return Promise.reject(error);
  //   });
  // logger.log('info', `(Service/mergeData.connectAndProcess) #
  // Data layer indicated SUCCESSFUL MONGO Connection # ${mongoConnection}`);
  logger.log('info', `(Service/mergeData.connectAndProcess) # Starting process for # ${processDate}`);
  

  await fetchCounts(processDate, snowConnection, table_name, where);
  
  try {
    await fetchAndUpdateHomeStore(processDate, snowConnection, table_name, columns, where, startBatch);
  } catch (e) {
    logger.log('error',`Catching error: ${e}`);
  } finally {
    logger.log('info',`Finished await fetchAndUpdateHomeStore`);
  }
  // mongoConnection.disconnect();
  return {
    message: 'successfully finished!'
  };
};

const mergeCustomerRecovery = async (table_name,columns,where,startBatch) => {
  //logger.log('info', `(Service/mergeData.connectAndProcess) # Parameters passed along  Table: ${table_name},  Column: ${columns}`);
  const tempDate = new Date(process.env.TARGET_DATE);
  const processDate = new Date(tempDate).toLocaleDateString(undefined, {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric'
  });
    // return new Promise( (resolve) => resolve(connectAndProcess(processDate)));
  return await connectAndProcess(processDate,table_name,columns,where,startBatch);
};

module.exports = {
  mergeCustomerRecovery
};
