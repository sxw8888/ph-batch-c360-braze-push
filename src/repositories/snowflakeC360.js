
const snowflake = require('snowflake-sdk');
const logger = require('../config/logger.js');
let sf_count =0;
let sf_query_count =0;

const snowflakeDBConnection = async (dboptions) => {
  const snowConnectionObj = snowflake.createConnection(dboptions);
  const connectionObj = await new Promise((resolve, reject) => snowConnectionObj.connect(
    (error, response) => {
      if (error) {
        logger.log('error', `(Repository/snowflakeC360.snowflakeDBConnection) # Unable to Connect to SNOWFLAKE DB : ${JSON.stringify(error)}`);
        return reject(error);
      }
      logger.log('info', `(Repository/snowflakeC360.snowflakeDBConnection) # Connected to SNOWFLAKE DB !!: ${JSON.stringify(response)}`);
      return resolve(response);
    }
  ));
  return connectionObj;
};

const getCounts = (processDate, snowConnection, table_name, where) => new Promise((resolve, reject) => {
  if(where) {
    snowConnection.execute({
      sqlText: `
                  SELECT 'MAB_DEMO' CATEGORY,COUNT(*) TOTALCOUNTS
                  FROM ${table_name}
		  WHERE EXTERNAL_ID IN (${where})
        `,
      complete: (error, stmt, rows) => {
        if (error) {
          logger.log('error', `(Repository/snowflakeC360.getCounts) # Unable to execute query on SNOWFLAKE DB`);
          reject(error);
        } else {
          resolve(rows);
        }
      }
    });
  } else {
    snowConnection.execute({
      sqlText: `
                  SELECT 'MAB_DEMO' CATEGORY,COUNT(*) TOTALCOUNTS
                  FROM ${table_name}                  
        `,
      complete: (error, stmt, rows) => {
        if (error) {
          logger.log('error', `(Repository/snowflakeC360.getCounts) # Unable to execute query on SNOWFLAKE DB`);
          reject(error);
        } else {
          resolve(rows);
        }
      }
    });
  }
});
const fetchHomeStoreUpdates = (processDate, snowConnection, offset, batchSize,  table_name, columns, where) => new Promise(
  (resolve, reject) => { 
    if(where) {
      snowConnection.execute({
        sqlText: `
                  SELECT ${columns}
                  FROM ${table_name}
		  WHERE EXTERNAL_ID IN (${where})
                  ORDER BY EXTERNAL_ID
                  LIMIT ${batchSize} OFFSET ${offset} 
              `,
        complete: (error, stmt, rows) => {
          if (error) {
            logger.log('error', `(Repository/snowflakeC360.fetchHomeStoreUpdates) # Unable to execute query on SNOWFLAKE DB`);
            reject(error);
          } else {
            resolve(rows);
          }
        }
      });
    } else {
      snowConnection.execute({
        sqlText: `
                  SELECT ${columns}
                  FROM ${table_name}
                  ORDER BY EXTERNAL_ID
                  LIMIT ${batchSize} OFFSET ${offset} 
              `,
        complete: (error, stmt, rows) => {
          if (error) {
            logger.log('error', `(Repository/snowflakeC360.fetchHomeStoreUpdates) # Unable to execute query on SNOWFLAKE DB`);
            reject(error);
          } else {
            resolve(rows);
          }
        }
      });
    }
  }
);
module.exports = {
  snowflakeDBConnection,
  getCounts,
  fetchHomeStoreUpdates
};
