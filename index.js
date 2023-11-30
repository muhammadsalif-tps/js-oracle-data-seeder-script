const oracledb = require('oracledb');
require('dotenv').config();
const { faker } = require('@faker-js/faker');

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
    id: faker.number.int({ min: 10, max: 100 }),
    order_id: faker.string.uuid(),
    transaction_id: faker.string.uuid(),
    transaction_type: faker.string.uuid(),
    service_number: faker.string.uuid(),
    customerid: faker.string.uuid(),
    qidcrn: faker.string.uuid(),
    account_number: faker.string.uuid(),
    email_address: faker.string.uuid(),
    ip_address: faker.string.uuid(),
    channel: faker.string.uuid(),
    subchannel: faker.string.uuid(),
    service_type: faker.string.uuid(),
    amount: faker.string.uuid(),
    commission_amount: faker.string.uuid(),
    vat: faker.string.uuid(),
    status: faker.string.uuid(),
    transaction_status: faker.string.uuid(),
    bank_status: faker.string.uuid(),
    fullfillment_responsecode: faker.string.uuid(),
    fullfillment_status: faker.string.uuid(),
    bank_description: faker.string.uuid(),
    pg_description: faker.string.uuid(),
    payment_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }), // '2026-05-16T02:22:53.002Z'
    fullfillment_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }), // '2026-05-16T02:22:53.002Z'
    refund_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }), // '2026-05-16T02:22:53.002Z'
    orphan_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }), // '2026-05-16T02:22:53.002Z'
    pgw_payment_type: faker.string.uuid(),
    payment_mode: faker.string.uuid(),
    card_number: faker.string.uuid(),
    card_scheme: faker.string.uuid(),
    card_expiry: faker.string.uuid(),
    card_holdername: faker.string.uuid(),
    issuer_bank: faker.string.uuid(),
    dpan: faker.string.uuid(),
    funding_method: faker.string.uuid(),
    mid: faker.string.uuid(),
    merchant_id: faker.string.uuid(),
    sub_merchant_id: faker.string.uuid(),
    currency: faker.string.uuid(),
    firstname: faker.string.uuid(),
    lastname: faker.string.uuid(),
    rrn: faker.string.uuid(),
    stan: faker.string.uuid(),
    acquirer_code: faker.string.uuid(),
    authcode: faker.string.uuid(),
    card_acceptor_terminal_id: faker.string.uuid(),
    inserted_by: faker.string.uuid(),
    modified_by: faker.string.uuid(),
    initiate_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }), // '2026-05-16T02:22:53.002Z'
    modified_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }), // '2026-05-16T02:22:53.002Z'
    instrument_type: faker.string.uuid(),
    response_code: faker.string.uuid(),
    stage1_status: faker.string.uuid(),
    stage2_status: faker.string.uuid(),
    stage3_status: faker.string.uuid(),
    stage4_status: faker.string.uuid(),
    stage1_status_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }), // '2026-05-16T02:22:53.002Z'
    stage2_status_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }), // '2026-05-16T02:22:53.002Z'
    stage3_status_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }), // '2026-05-16T02:22:53.002Z'
    stage4_status_datetim: faker.string.uuid()
  };
  const sql = `INSERT INTO IRIS_CUSTOM.CHANNEL_WISE_TRANSACTION_DAY 
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
                :merchant_id,
                :currency,
                :firstname,
                :lastname,
                :rrn,
                :stan,
                :acquirer_code,
                :authcode,
                :card_acceptor_terminal_id,
                :inserted_by
              )`;

  const binds = { ...inputData };
  // return dbInstance.execute(sql, binds, { autoCommit: true });
  return new Promise((resolve, reject) => {
    console.log("SQL", sql);
    console.log("DATA", inputData);
    resolve()
  });
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