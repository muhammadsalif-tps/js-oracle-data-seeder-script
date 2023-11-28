const oracledb = require('oracledb');
require('dotenv').config()

async function insertData(dbInstance) {
  try {
    // Sample data to be inserted
    const inputData = {
      transactions: 1111,
      day: "11/11/2023 00:00:00",
      channel: "9999",
    };

    // SQL statement for inserting data

    const sql = `INSERT INTO IRIS_CUSTOM.CHANNEL_WISE_TRANSACTION_DAY (TRANSACTIONS, DAY, CHANNEL) VALUES (:transactions, TO_DATE(:day, 'MM/DD/YYYY HH24:MI:SS'), :channel)`;

    // Bind parameters
    const binds = { ...inputData };

    // Execute the SQL statement
    const result = await dbInstance.execute(sql, binds, { autoCommit: true });

    console.log('Data inserted successfully:', result.rowsAffected);
  } catch (error) {
    console.error('Error inserting data:', error);
  }
}

const closeDbConnection = async (dbInstance) => {
  // Release the dbInstance
  try {
    await dbInstance.close();
  } catch (error) {
    console.error('Error closing dbInstance:', error);
  }
}

const createDbConnection = async () => {
  // Oracle DB connection parameters
  const dbConfig = {
    user: process.env.ORACLE_USERNAME,
    password: process.env.ORACLE_PASSWORD,
    connectString: process.env.ORACLE_CONNECTION_STRING,
  };
  // Connect to the Oracle Database
  let connection = null;
  try {
    connection = await oracledb.getConnection(dbConfig);
  } catch (error) {
    console.log(error)
  }
  return connection;
}

const start = async () => {
  const dbInstance = await createDbConnection();
  if (dbInstance) {
    await insertData(dbInstance);
    await closeDbConnection(dbInstance);
  }
  console.log("Connection closed");
  return;
}
start()
