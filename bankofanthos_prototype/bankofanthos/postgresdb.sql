-- Copyright 2020 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- transactions is a ledger of all financial transactions. Here, a financial
-- transaction is a transfer of funds from one account to another.
CREATE TABLE transactions (
  transaction_id BIGINT    GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  from_acct      CHAR(12)  NOT NULL,
  to_acct        CHAR(12)  NOT NULL,
  from_route     CHAR(9)   NOT NULL,
  to_route       CHAR(9)   NOT NULL,
  amount         INT       NOT NULL,
  timestamp      TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS transactions_from_idx ON transactions (from_acct, from_route, timestamp);
CREATE INDEX IF NOT EXISTS transactions_to_idx ON transactions (to_acct, to_route, timestamp);

-- transactions is an append-only ledger. Prevent updates and deletes.
CREATE RULE PREVENT_UPDATE AS
  ON UPDATE TO transactions
  DO INSTEAD NOTHING;

CREATE RULE PREVENT_DELETE AS
  ON DELETE TO transactions
  DO INSTEAD NOTHING;


CREATE TABLE balances (
  acctid        CHAR(12)  PRIMARY KEY,
  amount        INT       NOT NULL
);

CREATE TABLE currency (
  currency_code CHAR(3) PRIMARY KEY,
  value_usd FLOAT(8) NOT NULL
);
