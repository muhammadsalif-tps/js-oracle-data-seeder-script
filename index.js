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
    order_id: faker.string.sample({ min: 5, max: 6 }),
    transaction_id: faker.string.sample({ min: 5, max: 6 }),
    transaction_type: faker.string.sample({ min: 5, max: 6 }),
    service_number: faker.string.sample({ min: 5, max: 6 }),
    customerid: faker.string.sample({ min: 5, max: 6 }),
    qidcrn: faker.string.sample({ min: 5, max: 6 }),
    account_number: faker.string.sample({ min: 5, max: 6 }),
    email_address: faker.string.sample({ min: 5, max: 6 }),
    ip_address: faker.string.sample({ min: 5, max: 6 }),
    channel: faker.string.sample({ min: 5, max: 6 }),
    subchannel: faker.string.sample({ min: 5, max: 6 }),
    service_type: faker.string.sample({ min: 5, max: 6 }),
    amount: faker.string.sample({ min: 5, max: 6 }),
    commission_amount: faker.string.sample({ min: 5, max: 6 }),
    vat: faker.string.sample({ min: 5, max: 6 }),
    status: faker.string.sample({ min: 5, max: 6 }),
    transaction_status: faker.string.sample({ min: 5, max: 6 }),
    bank_status: faker.string.sample({ min: 5, max: 6 }),
    fullfillment_responsecode: faker.string.sample({ min: 5, max: 6 }),
    fullfillment_status: faker.string.sample({ min: 5, max: 6 }),
    bank_description: faker.string.sample({ min: 5, max: 6 }),
    pg_description: faker.string.sample({ min: 5, max: 6 }),
    payment_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }), // '2026-05-16T02:22:53.002Z'
    fullfillment_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }), // '2026-05-16T02:22:53.002Z'
    refund_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }), // '2026-05-16T02:22:53.002Z'
    orphan_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }), // '2026-05-16T02:22:53.002Z'
    pgw_payment_type: faker.string.sample({ min: 5, max: 6 }),
    payment_mode: faker.string.sample({ min: 5, max: 6 }),
    card_number: faker.string.sample({ min: 5, max: 6 }),
    card_scheme: faker.string.sample({ min: 5, max: 6 }),
    card_expiry: faker.string.sample({ min: 5, max: 6 }),
    card_holdername: faker.string.sample({ min: 5, max: 6 }),
    issuer_bank: faker.string.sample({ min: 5, max: 6 }),
    dpan: faker.string.sample({ min: 5, max: 6 }),
    funding_method: faker.string.sample({ min: 5, max: 6 }),
    mid: faker.string.sample({ min: 5, max: 6 }),
    merchant_id: faker.string.sample({ min: 5, max: 6 }),
    sub_merchant_id: faker.string.sample({ min: 5, max: 6 }),
    currency: faker.string.sample({ min: 1, max: 3 }),
    firstname: faker.string.sample({ min: 5, max: 6 }),
    lastname: faker.string.sample({ min: 5, max: 6 }),
    rrn: faker.string.sample({ min: 5, max: 6 }),
    stan: faker.string.sample({ min: 5, max: 6 }),
    acquirer_code: faker.string.sample({ min: 5, max: 6 }),
    authcode: faker.string.sample({ min: 5, max: 6 }),
    card_acceptor_terminal_id: faker.string.sample({ min: 5, max: 6 }),
    inserted_by: faker.string.sample({ min: 5, max: 6 }),
    modified_by: faker.string.sample({ min: 5, max: 6 }),
    initiate_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }), // '2026-05-16T02:22:53.002Z'
    modified_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }), // '2026-05-16T02:22:53.002Z'
    instrument_type: faker.string.sample({ min: 5, max: 6 }),
    response_code: faker.string.sample({ min: 5, max: 6 }),
    stage1_status: faker.string.sample({ min: 5, max: 6 }),
    stage2_status: faker.string.sample({ min: 5, max: 6 }),
    stage3_status: faker.string.sample({ min: 5, max: 6 }),
    stage4_status: faker.string.sample({ min: 5, max: 6 }),
    stage1_status_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }), // '2026-05-16T02:22:53.002Z'
    stage2_status_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }), // '2026-05-16T02:22:53.002Z'
    stage3_status_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }), // '2026-05-16T02:22:53.002Z'
    stage4_status_datetime: faker.date.between({ from: '2020-01-01T00:00:00.000Z', to: '2030-01-01T00:00:00.000Z' }) // '2026-05-16T02:22:53.002Z'
  };
  const sql = `INSERT INTO IRIS_CUSTOM.ODS_TRANSACTION_LOG_2 
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

async function middleFunction(totalRecords = 100000, maxRequest = 10, currentRecursionIteration = 1, failedRequests = []) {
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

    await Promise.all(_promises);
    console.log("currentRecursionIteration", currentRecursionIteration);
    console.log("DUMP==>", recordsToDump);
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