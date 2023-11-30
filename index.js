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

async function middleFunction(totalRecords = 100000000, maxRequest = 100, currentRecursionIteration = 1, failedRequests = []) {
  try {
    const recordsToDump = Math.min(maxRequest, totalRecords);
    const _promises = []

    for (let i = 0; i < recordsToDump; i++) {
      _promises.push(processData().catch((e) => {
        failedRequests.push({
          index: (currentRecursionIteration - 1) * maxRequest + i + 1,
          error: e,
        });
      }));
    }

    const result = await Promise.all(_promises);
    console.log("result", result);
    totalRecords = Math.abs(totalRecords - recordsToDump);

    if (totalRecords) {
      await middleFunction(totalRecords, maxRequest, currentRecursionIteration + 1, failedRequests);
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
  // const totalRecords = 100000000; // 10 crore
  // await middleFunction(totalRecords);
  await middleFunction();
  await dbInstance.close();
  console.log("Connection closed");
  return Promise.resolve();
}