const oracledb = require('oracledb');
require('dotenv').config();
const { faker } = require('@faker-js/faker');
const jsonData = require("./jsonData.json")

let dbInstance = null
const dbConfig = {
  user: process.env.ORACLE_USERNAME,
  password: process.env.ORACLE_PASSWORD,
  connectString: process.env.ORACLE_CONNECTION_STRING,
};

oracledb.getConnection(dbConfig).then((_db) => {
  dbInstance = _db;
  start();
}).catch((error) => {
  console.log("error in connection with db", error)
});

const createData = () => {
  const dataObj = {
    card_scheme: jsonData["cardScheme"][Math.floor(Math.random() * [jsonData["cardScheme"].length])],
    instrument_type: jsonData["instrumentType"][Math.floor(Math.random() * [jsonData["instrumentType"].length])],
    transaction_type: jsonData["transactionType"][Math.floor(Math.random() * [jsonData["transactionType"].length])],
    subchannel: jsonData["subChannel"][Math.floor(Math.random() * [jsonData["subChannel"].length])],
    merchant_id: jsonData["merchantId"][Math.floor(Math.random() * [jsonData["merchantId"].length])],
    transaction_status: jsonData["transactionStatusCodes"][Math.floor(Math.random() * [jsonData["transactionStatusCodes"].length])],
    mid: jsonData["MID"][Math.floor(Math.random() * [jsonData["MID"].length])],
    stages: jsonData["stages"][Math.floor(Math.random() * [jsonData["stages"].length])],
    order_transaction_id: jsonData["merchantId"][Math.floor(Math.random() * [jsonData["merchantId"].length])]
  }
  const data = {
    id: faker.number.int({ min: 10, max: 100000 }),
    order_id: `${dataObj.order_transaction_id}`,
    transaction_id: `${dataObj.order_transaction_id}`,
    transaction_type: dataObj.transaction_type,
    service_number: `${faker.number.int()}`,
    customerid: `${faker.number.int()}`,
    qidcrn: `${faker.number.int()}`,
    account_number: `${faker.number.int({ min: 10000000000, max: 99999999999 })}`,
    email_address: faker.internet.email(),
    ip_address: faker.internet.ipv4(),
    channel: jsonData['channel'][dataObj.subchannel],
    subchannel: dataObj.subchannel,
    service_type: "DirectTopup",
    amount: faker.number.float({ min: 10000, max: 10000000, precision: 0.001 }),
    commission_amount: null,
    vat: null,
    status: jsonData["transactionStatus"][dataObj.transaction_status],
    transaction_status: dataObj.transaction_status,
    bank_status: jsonData["transactionStatus"][dataObj.transaction_status],
    fullfillment_responsecode: null,
    fullfillment_status: jsonData["transactionStatus"][dataObj.transaction_status],
    bank_description: null,
    pg_description: null,
    payment_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }), // '2026-05-16T02:22:53.002Z'
    fullfillment_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }), // '2026-05-16T02:22:53.002Z'
    refund_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }), // '2026-05-16T02:22:53.002Z'
    orphan_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }), // '2026-05-16T02:22:53.002Z'
    pgw_payment_type: dataObj.instrument_type,
    payment_mode: "OREEDOSERVICE",
    card_number: null,
    card_scheme: dataObj.card_scheme,
    card_expiry: `${new Date().getMonth()}/${new Date().getFullYear()}`,
    card_holdername: faker.person.firstName(),
    issuer_bank: "QNB",
    dpan: null,
    funding_method: null,
    mid: dataObj.mid,
    merchant_id: dataObj.merchant_id,
    sub_merchant_id: `${dataObj.merchant_id}-${dataObj.transaction_type}`,
    currency: `QAR`,
    firstname: faker.person.firstName(),
    lastname: faker.person.lastName(),
    rrn: `${faker.number.int({ min: 10000000000000, max: 99999999999999 })}`,
    stan: `${faker.number.int({ min: 100000, max: 999999 })}`,
    acquirer_code: `00`,
    authcode: `${faker.number.int({ min: 10000, max: 999999 })}`,
    card_acceptor_terminal_id: "TESTORPRQNB01",
    inserted_by: null,
    modified_by: null,
    initiate_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }), // '2026-05-16T02:22:53.002Z'
    modified_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }), // '2026-05-16T02:22:53.002Z'
    instrument_type: dataObj.instrument_type,
    response_code: jsonData["transactionStatusXresponseCode"][dataObj.transaction_status],
    stage1_status: dataObj.stages,
    stage2_status: dataObj.stages === "0000" ? "0000" : null,
    stage3_status: dataObj.stages === "0000" ? "0000" : null,
    stage4_status: dataObj.stages === "0000" ? "0000" : null,
    stage1_status_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }), // '2026-05-16T02:22:53.002Z'
    stage2_status_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }), // '2026-05-16T02:22:53.002Z'
    stage3_status_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }), // '2026-05-16T02:22:53.002Z'
    stage4_status_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }) // '2026-05-16T02:22:53.002Z'
  };
  return data;
}

const processData = () => {
  const inputData = createData();
  const sql = `INSERT INTO IRIS_CUSTOM.TRANSACTION_LOG_V2 
              (
                ID,
                ORDER_ID,
                TRANSACTION_ID,
                TRANSACTION_TYPE,
                SERVICE_NUMBER,
                CUSTOMERID,
                QIDCRN,
                ACCOUNT_NUMBER,
                EMAIL_ADDRESS,
                IP_ADDRESS,
                CHANNEL,
                SUBCHANNEL,
                SERVICE_TYPE,
                AMOUNT,
                COMMISSION_AMOUNT,
                VAT,
                STATUS,
                TRANSACTION_STATUS,
                BANK_STATUS,
                FULLFILLMENT_RESPONSECODE,
                FULLFILLMENT_STATUS,
                BANK_DESCRIPTION,
                PG_DESCRIPTION,
                PAYMENT_DATETIME,
                FULLFILLMENT_DATETIME,
                REFUND_DATETIME,
                ORPHAN_DATETIME,
                PGW_PAYMENT_TYPE,
                PAYMENT_MODE,
                CARD_NUMBER,
                CARD_SCHEME,
                CARD_EXPIRY,
                CARD_HOLDERNAME,
                ISSUER_BANK,
                DPAN,
                FUNDING_METHOD,
                MID,
                MERCHANT_ID,
                SUB_MERCHANT_ID,
                CURRENCY,
                FIRSTNAME,
                LASTNAME,
                RRN,
                STAN,
                ACQUIRER_CODE,
                AUTHCODE,
                CARD_ACCEPTOR_TERMINAL_ID,
                INSERTED_BY,
                MODIFIED_BY,
                INITIATE_DATETIME,
                MODIFIED_DATETIME,
                INSTRUMENT_TYPE,
                RESPONSE_CODE,
                STAGE1_STATUS,
                STAGE2_STATUS,
                STAGE3_STATUS,
                STAGE4_STATUS,
                STAGE1_STATUS_DATETIME,
                STAGE2_STATUS_DATETIME,
                STAGE3_STATUS_DATETIME,
                STAGE4_STATUS_DATETIME             
              )
              VALUES 
              (
                :id,
                :order_id,
                :transaction_id,
                :transaction_type,
                :service_number,
                :customerid,
                :qidcrn,
                :account_number,
                :email_address,
                :ip_address,
                :channel,
                :subchannel,
                :service_type,
                :amount,
                :commission_amount,
                :vat,
                :status,
                :transaction_status,
                :bank_status,
                :fullfillment_responsecode,
                :fullfillment_status,
                :bank_description,
                :pg_description,
                :payment_datetime,
                :fullfillment_datetime,
                :refund_datetime,
                :orphan_datetime,
                :pgw_payment_type,
                :payment_mode,
                :card_number,
                :card_scheme,
                :card_expiry,
                :card_holdername,
                :issuer_bank,
                :dpan,
                :funding_method,
                :mid,
                :merchant_id,
                :sub_merchant_id,
                :currency,
                :firstname,
                :lastname,
                :rrn,
                :stan,
                :acquirer_code,
                :authcode,
                :card_acceptor_terminal_id,
                :inserted_by,
                :modified_by,
                :initiate_datetime,
                :modified_datetime,
                :instrument_type,
                :response_code,
                :stage1_status,
                :stage2_status,
                :stage3_status,
                :stage4_status,
                :stage1_status_datetime,
                :stage2_status_datetime,
                :stage3_status_datetime,
                :stage4_status_datetime               
              )`;
  const binds = { ...inputData };
  return dbInstance.execute(sql, binds, { autoCommit: true });
}

function calculateTimePassed(startTime, endTime) {
  const timeDiff = endTime - startTime;
  const seconds = Math.floor(timeDiff / 1000);
  const minutes = Math.floor(seconds / 60);
  return `${minutes} minutes and ${seconds % 60} seconds`;
}

async function middleFunction(
  totalRecords = 100000000,
  maxRequest = 10000,
  dataInserted = 0,
  failedRequests = [],
  startTime = new Date()
) {
  try {
    console.log("records remaining to insert", totalRecords)
    const recordsToDump = Math.min(maxRequest, totalRecords);
    const _promises = []

    for (let i = 0; i < recordsToDump; i++) {
      _promises.push(processData().catch((e) => {
        failedRequests.push({
          index: (dataInserted - 1) * maxRequest + i + 1,
          error: e,
        });
      }));
    }

    await Promise.all(_promises);
    dataInserted += _promises.length;
    console.log("Data inserted", dataInserted);
    totalRecords = Math.abs(totalRecords - recordsToDump);
    const timePassed = calculateTimePassed(startTime, new Date());
    console.log(`Time passed: ${timePassed}`);
    console.log("<===============================>")
    if (totalRecords) {
      await middleFunction(totalRecords, maxRequest, dataInserted, failedRequests, startTime);
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
  console.log("database connected");
  await middleFunction();
  await dbInstance.close();
  console.log("Connection closed");
  return Promise.resolve();
}