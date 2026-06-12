"use strict";

const mysql = require("mysql2/promise");

const DB_NAME = process.env.DB_NAME || "respublica_gesetze";

let pool;

function getPool() {
  if (!pool) {
    pool = mysql.createPool({
      host: process.env.DB_HOST || "localhost",
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD || "",
      database: DB_NAME,
      waitForConnections: true,
      connectionLimit: 10,
      charset: "utf8mb4",
    });
  }
  return pool;
}

module.exports = { getPool, DB_NAME };
