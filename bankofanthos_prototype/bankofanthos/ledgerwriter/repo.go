// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ledgerwriter

import (
	"bankofanthos_prototype/bankofanthos/model"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type transactionRepository struct {
	db *gorm.DB
}

func newTransactionRepository(databaseURI string) (*transactionRepository, error) {
	db, err := gorm.Open(postgres.Open(databaseURI))
	if err != nil {
		return nil, err
	}
	return &transactionRepository{db: db}, nil
}

func (r *transactionRepository) save(transaction *model.Transaction) error {
	return r.db.Create(transaction).Error
}

// update Balance updates acctId to amount.
// Delete existing record if there is any, then insert the updated amount.
func (r *transactionRepository) updateBalance(acctId string, amount int64) error {
	deleteSql := `
	DELETE FROM balances Where acctid = ?;
	`
	delete := r.db.Exec(deleteSql, acctId)
	if delete.Error != nil {
		return delete.Error
	}

	insertSql := `
	INSERT INTO balances(acctid, amount) VALUES(?, ?);
	`
	insert := r.db.Exec(insertSql, acctId, amount)
	return insert.Error
}

func (r *transactionRepository) getAllCurrency(maxCurrency int) (map[string]float32, error) {
	rows, err := r.db.Raw("SELECT currency_code, value_usd FROM Currency ORDER BY currency_code LIMIT ?", maxCurrency).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	m := map[string]float32{}
	for rows.Next() {
		var code string
		var valueUsd float32
		if err := rows.Scan(&code, &valueUsd); err != nil {
			return nil, err
		}
		m[code] = valueUsd
	}

	return m, nil
}
