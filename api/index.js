"use strict";

const path = require("path");
const express = require("express");
const cors = require("cors");

const { notFoundHandler, errorHandler } = require("./lib/errors");

require("dotenv").config({ path: path.join(__dirname, "..", ".env") });

const PORT = Number.parseInt(process.env.PORT || "3002", 10);

const app = express();
app.use(cors());
app.use(express.json());

app.use("/api", require("./routes/gesetze"));
app.use("/api", require("./routes/bundestag"));
app.use("/api", require("./routes/urteile"));
app.use("/api", require("./routes/eu"));
app.use("/api", require("./routes/lobby"));
app.use("/api", require("./routes/wahlen"));
app.use("/api", require("./routes/world"));

app.use(notFoundHandler);
app.use(errorHandler);

app.listen(PORT, () => {
  console.log(`API listening on port ${PORT}`);
});
