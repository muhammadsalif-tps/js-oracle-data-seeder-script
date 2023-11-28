const oracledb = require('oracledb');
require('dotenv').config();

let dbInstance = null
const dbConfig = {
  user: process.env.ORACLE_USERNAME,
  password: process.env.ORACLE_PASSWORD,
  connectString: process.env.ORACLE_CONNECTION_STRING,
};

oracledb.getConnection(dbConfig).then((_db) => {
  dbInstance = _db;
  start();
});

const processData = () => {
  const inputData = {
    transactions: 1111,
    day: "11/11/2023 00:00:00",
    channel: "9999",
  };
  const sql = `INSERT INTO IRIS_CUSTOM.CHANNEL_WISE_TRANSACTION_DAY (TRANSACTIONS, DAY, CHANNEL) VALUES (:transactions, TO_DATE(:day, 'MM/DD/YYYY HH24:MI:SS'), :channel)`;
  const binds = { ...inputData };
  return dbInstance.execute(sql, binds, { autoCommit: true });
}

async function middleFunction(array = [], maxRequest = 2, failedRequests = []) {
  try {
    const _array = array.splice(0, maxRequest);
    const _promises = _array.map((_data, ind) => {
      return processData().catch((e) => {
        failedRequests.push({
          // dbInstance: userSetting._id.toString(),
          index: ind,
          error: e,
        });
      });
    });
    const result = await Promise.all(_promises);
    console.log("result", result);

    if (array && array.length) {
      await middleFunction(array, maxRequest, failedRequests);
    }

    return Promise.resolve(failedRequests);
  } catch (error) {
    console.error('Error inserting data:', error);
  }
}

const start = async () => {
  if (!dbInstance) {
    console.log("Connection to db failed");
    return Promise.resolve();
  }
  const _temp = new Array(7).fill(0);
  await middleFunction(_temp);
  await dbInstance.close();
  console.log("Connection closed");
  return Promise.resolve();
}